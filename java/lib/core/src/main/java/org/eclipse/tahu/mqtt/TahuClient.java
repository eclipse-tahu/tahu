/********************************************************************************
 * Copyright (c) 2022-2023 Cirrus Link Solutions and others
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *   Cirrus Link Solutions - initial implementation
 ********************************************************************************/

package org.eclipse.tahu.mqtt;

import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Deque;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;

import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.eclipse.paho.client.mqttv3.util.Debug;
import org.eclipse.tahu.exception.TahuErrorCode;
import org.eclipse.tahu.exception.TahuException;
import org.eclipse.tahu.message.model.StatePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * An Custom MQTT client.
 */
public class TahuClient implements MqttCallbackExtended {

	private static Logger logger = LoggerFactory.getLogger(TahuClient.class.getName());

	private static final long DEFAULT_CONNECT_RETRY_INTERVAL = 1000;
	private static final long DEFAULT_CONNECT_MONITOR_INTERVAL = 10000;
	private static final long DEFAULT_CONNECT_ATTEMPT_TIMEOUT = 30000;
	private static final int DEFAULT_PUBLISH_BUFFER_CAPACITY = 10000;
	/*
	 * The second bound on the publish buffer, in bytes of payload.
	 *
	 * A count alone does not bound heap, which is the dimension that OOMs: payloads are held by reference and are
	 * whatever the caller passes, so 10,000 messages is 10,000 x an unknown. Measured, 100 x 1MB QoS 1 publishes were
	 * admitted without complaint and retained ~200MB.
	 *
	 * Deliberately a fixed default rather than a fraction of Runtime.maxMemory(): a heap-relative bound behaves
	 * differently on a laptop and on a gateway, so the same traffic would be accepted in one place and rejected in
	 * the other, and a support case would not reproduce. 128MB is small against any heap a broker-facing JVM runs
	 * with, and large enough that only genuinely large payloads under a sustained stall reach it.
	 */
	private static final long DEFAULT_PUBLISH_BUFFER_BYTE_CAPACITY = 128L * 1024 * 1024;
	private static final long PUBLISH_BUFFER_POLL_INTERVAL = 250;
	/*
	 * The retry budget for a buffered publish that fails to send, as a window of wall clock rather than a count of
	 * attempts. The failures worth retrying - REASON_CODE_MAX_INFLIGHT when Tahu's permit count has drifted above
	 * Paho's window, REASON_CODE_CLIENT_NOT_CONNECTED across a brief drop - are transient and clear on their own, so
	 * the budget only means anything if it spans time. An attempt count alone did not: nothing on the failure path
	 * delays, so three attempts were consumed in about two milliseconds and a blip destroyed the message.
	 *
	 * The window is deliberately short, because it is calibrated to a race and not to an outage. 32202 is thrown when
	 * Tahu's permit count is ahead of Paho's actualInFlight, and the two go back into step as soon as Paho finishes
	 * the acknowledgement it is already processing - microseconds, not seconds. Where the drift is structural rather
	 * than a race the condition never clears on its own, and where the client is simply not connected the drain's own
	 * isConnected() check handles it on the next pass; in neither case does a longer window help, it only holds up
	 * everything else in the buffer, because the backoff sleeps the drain thread. At 500ms a message gets three
	 * attempts spread over roughly 750ms of wall clock.
	 *
	 * MAX_BUFFERED_PUBLISH_ATTEMPTS and MAX_BUFFERED_PUBLISH_RETRY_DELAY cannot bind at these values, and that is
	 * worth saying plainly rather than leaving a reader to work out why the attempt count never appears in a log.
	 * The delay is min(POLL_INTERVAL * attempts, RETRY_DELAY), so the third failure always lands past a 500ms
	 * window: attempts stop at 3, and reaching 10 of them would take 4250ms of backoff. They are kept because they
	 * are the bounds that start to matter the moment the window or the backoff step is retuned - a shorter step
	 * fits more attempts into the window, and the cap is what stops the last of them waiting longer than the window
	 * itself. The window is what decides today.
	 */
	private static final long MAX_BUFFERED_PUBLISH_RETRY_WINDOW = 500;
	private static final int MAX_BUFFERED_PUBLISH_ATTEMPTS = 10;
	private static final long MAX_BUFFERED_PUBLISH_RETRY_DELAY = 500;

	/* How often the start of buffering may be logged at WARN - see lastBufferingWarnTime. */
	private static final long BUFFERING_WARN_INTERVAL = 30000;

	/*
	 * How long publishBirthMessage() will wait for backpressure to clear before it gives up on the session, and how
	 * often it looks. An exhausted window or a queued message is the ordinary condition the publish buffer exists to
	 * absorb and it clears on the next acknowledgement, so refusing immediately tore sessions down over a blip.
	 *
	 * The poll is deliberately fine grained. The wait runs on the BIRTH recovery worker with clientLock released, so
	 * the interval is not about lock hold time; it is about how quickly a blip that has already cleared is noticed,
	 * since the whole budget is spent before the session is given up on.
	 */
	private static final long BIRTH_BACKPRESSURE_WAIT = 500;
	private static final long BIRTH_BACKPRESSURE_POLL = 50;

	/*
	 * How long disconnect() will wait for the publish buffer to drain before publishing the LWT. Short on purpose:
	 * disconnect() runs per client, so a transmitter backed by a pool pays this serially, and it only elapses when
	 * the MQTT server is genuinely stuck. At 2 seconds a three client pool adds at most 6 seconds to a module stop.
	 */
	private static final long LWT_DRAIN_TIMEOUT = 2000;

	/*
	 * The pre-LWT flush polls on its own interval, not PUBLISH_BUFFER_POLL_INTERVAL, which the drain's retry ladder
	 * also uses. At 250ms a 600ms no-progress window was really 750ms - the first poll at or past it - so the
	 * threshold could not mean what it said and the effective budget was 750ms of a nominal 2000ms.
	 */
	private static final long DRAIN_WAIT_POLL = 50;

	/*
	 * How long the flush tolerates seeing no change before giving up, as a fraction of its own budget rather than a
	 * fixed figure. The buffer drains in steps - a permit returns, depth falls - so an interval with neither moving
	 * means nothing is servicing it, whichever thread waits and whatever lock is in the way. Half the budget keeps
	 * it well clear of an acknowledgement round trip on a slow server, which is the case this must not mistake for
	 * a stalled one, while still recovering the other half when nothing is coming.
	 */
	private static final long DRAIN_NO_PROGRESS_FRACTION = 2;
	private static final long DRAIN_NO_PROGRESS_FLOOR = 500;

	private Thread connectRunnableThread;
	private ConnectRunnable connectRunnable;
	private long connectRetryInterval;
	private long connectAttemptTimeout;

	/*
	 * Tracks the state of the connection attempts.
	 */
	private ConnectingState state = new ConnectingState();

	/*
	 * birth/death properties
	 */
	private boolean useSparkplugStatePayload;
	private Long lastStateDeathPayloadTimestamp;
	private String birthTopic;
	private byte[] birthPayload;
	private boolean birthRetain;
	private String lwtTopic;
	private byte[] lwtPayload;
	private int lwtQoS;
	private boolean lwtRetain;
	private IMqttDeliveryToken lwtDeliveryToken;
	private Object lwtDeliveryLock = new Object();

	/*
	 * The Asynchronous MQTT Client and MQTTConnectOptions
	 */
	private volatile TahuMqttAsyncClient client = null;
	MqttConnectOptions connectOptions = null;

	/*
	 * Other standard MQTT parameters.
	 */
	private final MqttServerUrl mqttServerUrl;
	private final MqttServerName mqttServerName;
	private final MqttClientId clientId;
	private String username;
	private String password;
	private final boolean cleanSession;
	private final int keepAlive;

	/*
	 * The callback client
	 */
	private ClientCallback callback;

	/**
	 * A list of topics the client has subscribed on
	 */
	private final SortedMap<String, Integer> subscriptions = new ConcurrentSkipListMap<>();

	/*
	 * Odds/ends
	 */
	/*
	 * Volatile because the deferred connect below reads it as a shutting-down signal, and the write comes from an
	 * application thread holding no lock - HostApplication.shutdown() sets it false and then disconnects. Without
	 * that edge the guard can read a stale true indefinitely, which is precisely the case it exists to refuse.
	 */
	private volatile boolean autoReconnect;
	private RandomStartupDelay randomStartupDelay;

	/*
	 * The maximum number of in-flight (pending) messages for the client to store. If this maximum is, publishes will
	 * fail with and INTERNAL_ERROR: Caused by: org.eclipse.paho.client.mqttv3.MqttException: Too many publishes in
	 * progress
	 */
	private int maxInFlightMessages = 32768;

	/*
	 * The maximum number of topics per individual subscribe message.
	 */
	private int maxTopicsPerSubscribe = 256;

	/*
	 * The maximum number of messages held in the FIFO publish buffer.
	 *
	 * The buffer MUST stay bounded. An unbounded one only moves the failure from thread exhaustion to heap exhaustion
	 * under a sustained server stall. At capacity the newest publish is rejected with a TahuException so the caller can
	 * fall back to its own store-and-forward rather than lose the message silently.
	 */
	private volatile int publishBufferCapacity = DEFAULT_PUBLISH_BUFFER_CAPACITY;

	/*
	 * The same bound expressed in bytes of buffered payload - see DEFAULT_PUBLISH_BUFFER_BYTE_CAPACITY. Whichever
	 * bound is reached first rejects the publish, and a single payload larger than the whole budget is rejected
	 * outright so one oversized message cannot occupy the buffer alone.
	 */
	private volatile long publishBufferByteCapacity = DEFAULT_PUBLISH_BUFFER_BYTE_CAPACITY;

	/*
	 * The FIFO publish buffer, the lock guarding both it and publish ordering, and the thread that drains it.
	 *
	 * publishOrderLock guards the decision "publish now or queue" so that, in normal operation, no message
	 * overtakes one already queued.
	 *
	 * KNOWN GAP, accepted deliberately: when a buffered send fails, the lock is released between the failed publish and
	 * the requeueOrDrop() that puts the message back at the head, so a concurrent publisher can see an empty buffer and
	 * go inline ahead of it. Ordering is therefore best effort, not guaranteed. This is accepted rather than fixed
	 * because MQTT provides no ordering guarantee across an in-flight window greater than one anyway, and the callers
	 * that care about sequence - Sparkplug NDATA/DDATA - publish at QoS 0 and never enter this buffer.
	 *
	 * Lock order is always publishOrderLock -> messageLock; deliveryComplete() takes messageLock only, so there is no
	 * cycle and permits can always be returned.
	 */
	private final Object publishOrderLock = new Object();
	private final Deque<BufferedPublish> publishBuffer = new ArrayDeque<>();

	/*
	 * Running total of the payload bytes in publishBuffer, guarded by publishOrderLock like the buffer itself.
	 *
	 * Maintained ONLY by bufferAddLast, bufferAddFirst, bufferRemoveFirst and bufferClear below - nothing else may
	 * touch publishBuffer directly. A sum that drifts from the deque is the same class of defect as a permit drifting
	 * above Paho's window: it does not fail where it happened, it fails later, as a client that refuses everything
	 * against a buffer that looks empty. The requeue path is where that would start, since a failed send removes an
	 * entry from the head and puts it back.
	 */
	private long publishBufferBytes;
	/*
	 * Cumulative count of publishes rejected because the buffer was full. Guarded by publishOrderLock like the
	 * buffer itself. Never reset while the client lives - it is a lifetime counter, not a gauge, so a consumer
	 * can see that loss happened even if the buffer has since drained.
	 */
	private long publishBufferRejectedMessageCount;

	/*
	 * Counted separately from publishBufferRejectedMessageCount, because the two mean different things to whoever is
	 * looking at them. A rejection is a refusal at the door: publish() threw, so the caller knows and can store the
	 * message itself. A discard is a message this client accepted - publish() returned null, meaning queued rather
	 * than lost - and then threw away. Only the second is a loss the caller could not see coming.
	 */
	private long publishBufferDiscardedMessageCount;

	/*
	 * When the last "buffering has started" warning was logged, so a client that oscillates between empty and one
	 * queued message does not log a line per publish. At a publish rate that balances against the acknowledgement
	 * rate that is exactly what happens, and the warning is worth keeping - it is where an operator sees that
	 * backpressure began - but not at one line per message. Guarded by publishOrderLock like the rest.
	 */
	private long lastBufferingWarnTime;

	private PublishBufferDrain publishBufferDrain;
	private Thread publishBufferDrainThread;

	/*
	 * Retrying or escalating a BIRTH must not happen on the thread that delivers the acknowledgements it is waiting
	 * for. Paho dispatches deliveryComplete(), messageArrived() and connectComplete() from a single callback thread,
	 * and two of the three routes into publishBirthMessage() arrive on it - so a wait taken there blocks the only
	 * thread that could satisfy it, and a teardown taken there re-enters connect() from inside connectComplete().
	 */
	private Thread birthRecoveryThread;

	/*
	 * A connect refused because a teardown held the gate.
	 *
	 * Read and written under clientLock, together with the gate itself. Testing the gate and recording the intent as
	 * separate steps is the same lost update the flag exists to prevent: a teardown can release between them, find
	 * nothing pending, and leave the caller having recorded an intent nobody will act on.
	 *
	 * The intent is the whole of the state. There is deliberately no handle on the worker that replays it: a worker
	 * past the gate is running a connect that cannot be cancelled from outside - connect() never tests the interrupt
	 * flag - so holding a handle only offers the interrupt, which cancels nothing and breaks the timed waits the
	 * teardown inside that connect depends on.
	 */
	private boolean deferredConnectPending;
	private TahuMqttAsyncClient birthRecoverySession;

	private Date connectTime;
	private Date disconnectTime;
	private Date onlineDate;
	private Date offlineDate;
	private double totalUptime;
	private double totalDowntime;
	private int connectionCount = 0; // # of Edge Nodes connected to this MQTT Client's Broker
	private boolean doLatencyCheck = false;
	private long numMesgsArrived = 0;
	private long lastNumMesgsArrived = 0;

	/*
	 * Read by publish() to tell an interrupt that is part of an intentional disconnect from one that has silently
	 * dropped a message, so it is written and read from different threads.
	 */
	/*
	 * How many teardowns are in flight, not whether one is. A plain boolean has no owner: a second teardown - or one
	 * that returns early having claimed nothing - clears it while the first is still closing its Paho client, which
	 * reopens every guard that reads it for exactly the window they exist to cover. Reads true until the last
	 * teardown has finished its close.
	 */
	private final AtomicInteger teardownsInFlight = new AtomicInteger();

	/*
	 * Set by publishLwt() so disconnect() can tell whether the death certificate actually reached the MQTT client.
	 * False means every attempt failed, including the QoS 0 fallback - and the only remaining way to get one
	 * published is to let the MQTT server publish the Will, which means NOT sending a DISCONNECT packet.
	 */
	private volatile boolean lwtPublishSucceeded = true;

	/*
	 * Whether the last LWT attempt had to fall back to QoS 0. A downgraded death certificate is published but never
	 * acknowledged, so it does not count as success for the purpose of deciding whether to send a clean DISCONNECT -
	 * see publishLwtWithFallback() and the last resort in disconnect().
	 */
	private volatile boolean lwtDowngradedToQos0;

	private Object clientLock = new Object();
	private ConnectionMonitorThread connectionMonitorThread;

	private boolean trackFirstConnection = false;
	private boolean firstConnection = true;
	private boolean resubscribed = false;

	// Whether or not the BIRTH should be published on connect and controls the STATE of the client
	private boolean onlineState;

	// volatile: written in connect() under messageLock, but read under publishOrderLock in publishOrBuffer() and
	// with no lock at all by the publish buffer drain thread. Without volatile there is no happens-before edge to
	// those readers, so a reconnect's replacement instance may not be seen promptly.
	private volatile Semaphore semaphore;
	private volatile Set<Integer> lockedMessageSet;
	private final Object messageLock;

	public TahuClient(final MqttClientId clientId, final MqttServerName mqttServerName,
			final MqttServerUrl mqttServerUrl, final String username, final String password, boolean cleanSession,
			int keepAlive, ClientCallback callback, RandomStartupDelay randomStartupDelay, boolean onlineState) {
		this.mqttServerUrl = mqttServerUrl;
		this.mqttServerName = mqttServerName;
		this.clientId = clientId;
		this.username = username;
		this.password = password;
		this.cleanSession = cleanSession;
		this.keepAlive = keepAlive;
		this.callback = callback;
		this.randomStartupDelay = randomStartupDelay;
		this.lwtRetain = false;
		this.birthRetain = false;
		this.autoReconnect = true;
		this.setConnectRetryInterval(DEFAULT_CONNECT_RETRY_INTERVAL);
		this.setConnectAttemptTimeout(DEFAULT_CONNECT_ATTEMPT_TIMEOUT);
		this.renewDisconnectTime();
		this.renewOnlineDate();
		this.renewOfflineDate();
		this.onlineState = onlineState;
		this.messageLock = new Object();
	}

	public TahuClient(final MqttClientId clientId, final MqttServerName mqttServerName,
			final MqttServerUrl mqttServerUrl, String username, String password, boolean cleanSession, int keepAlive,
			ClientCallback callback, RandomStartupDelay randomStartupDelay, boolean useSparkplugStatePayload,
			String birthTopic, byte[] birthPayload, String lwtTopic, byte[] lwtPayload, int lwtQoS,
			boolean onlineState) {
		this(clientId, mqttServerName, mqttServerUrl, username, password, cleanSession, keepAlive, callback,
				randomStartupDelay, onlineState);
		this.setLifecycleProps(useSparkplugStatePayload, birthTopic, birthPayload, false, lwtTopic, lwtPayload, lwtQoS,
				false);
	}

	public TahuClient(final MqttClientId clientId, final MqttServerName mqttServerName,
			final MqttServerUrl mqttServerUrl, String username, String password, boolean cleanSession, int keepAlive,
			ClientCallback callback, RandomStartupDelay randomStartupDelay, boolean onlineState,
			boolean useSparkplugStatePayload, String birthTopic, byte[] birthPayload, boolean birthRetain,
			String lwtTopic, byte[] lwtPayload, int lwtQoS, boolean lwtRetain) {
		this(clientId, mqttServerName, mqttServerUrl, username, password, cleanSession, keepAlive, callback,
				randomStartupDelay, onlineState);
		this.setLifecycleProps(useSparkplugStatePayload, birthTopic, birthPayload, birthRetain, lwtTopic, lwtPayload,
				lwtQoS, lwtRetain);
	}

	/**
	 * Sets the properties relating to client life cycle events such as LWT and Birth topics and payloads.
	 * 
	 * @param birthTopic the topic to publish birth certificates on
	 * @param birthPayload the payload of a birth certificate
	 * @param birthRetain whether to retain birth certificate messages
	 * @param lwtTopic the topic to publish LWT on
	 * @param lwtPayload the payload of an LWT
	 * @param lwtRetain whether to retain LWT messages
	 */
	private void setLifecycleProps(boolean useSparkplugStatePayload, String birthTopic, byte[] birthPayload,
			boolean birthRetain, String lwtTopic, byte[] lwtPayload, int lwtQoS, boolean lwtRetain) {
		this.useSparkplugStatePayload = useSparkplugStatePayload;
		this.birthTopic = birthTopic;
		this.birthPayload = birthPayload;
		this.birthRetain = birthRetain;
		this.lwtTopic = lwtTopic;
		this.lwtPayload = lwtPayload;
		this.lwtQoS = lwtQoS;
		this.lwtRetain = lwtRetain;

	}

	protected MqttConnectOptions getMqttConnectOptions() {
		return connectOptions;
	}

	protected void setMqttConnectOptions(MqttConnectOptions connectOptions) {
		this.connectOptions = connectOptions;
	}

	public int getAvailablePublishPermits() {
		return semaphore != null ? semaphore.availablePermits() : 0;
	}

	public long getNumMesgsArrived() {
		return numMesgsArrived;
	}

	public long getMesgsArrivedDelta() {
		// Returns the number of messages arrived since last called.
		long delta = numMesgsArrived - lastNumMesgsArrived;
		lastNumMesgsArrived = numMesgsArrived;
		return delta;
	}

	public void clearMesgArrivedCount() {
		numMesgsArrived = 0;
		lastNumMesgsArrived = 0;
	}

	public void setMaxInflightMessages(int max) {
		this.maxInFlightMessages = max;
	}

	public int getMaxInflightMessages() {
		return this.maxInFlightMessages;
	}

	public void setPublishBufferCapacity(int publishBufferCapacity) {
		this.publishBufferCapacity = publishBufferCapacity;
	}

	public int getPublishBufferCapacity() {
		return this.publishBufferCapacity;
	}

	/**
	 * Sets the second bound on the publish buffer: the total payload bytes it may hold.
	 *
	 * The count bound does not bound heap. Payloads are held by reference and sized by the caller, so a capacity of
	 * 10,000 messages is 10,000 times an unknown - which is the failure this buffer exists to prevent, arrived at
	 * from the other direction. Whichever bound is reached first rejects the publish.
	 *
	 * @param publishBufferByteCapacity total payload bytes the buffer may hold, default 128MB
	 */
	public void setPublishBufferByteCapacity(long publishBufferByteCapacity) {
		this.publishBufferByteCapacity = publishBufferByteCapacity;
	}

	public long getPublishBufferByteCapacity() {
		return this.publishBufferByteCapacity;
	}

	/**
	 * @return the payload bytes currently held in the publish buffer. The companion to
	 *         {@link #getPublishBufferDepth()}, and the one that predicts heap: depth counts messages, which says
	 *         nothing about what they cost to hold.
	 */
	public long getPublishBufferBytes() {
		synchronized (publishOrderLock) {
			return publishBufferBytes;
		}
	}

	public void setDoLatencyCheck(boolean state) {
		doLatencyCheck = state;
	}

	public boolean getDoLatencyCheck() {
		return doLatencyCheck;
	}

	public void clearConnectionCount() {
		connectionCount = 0;
	}

	public void incrementConnectionCount() {
		connectionCount++;
	}

	public int getConnectionCount() {
		return connectionCount;
	}

	public MqttServerUrl getMqttServerUrl() {
		return mqttServerUrl;
	}

	public MqttServerName getMqttServerName() {
		return mqttServerName;
	}

	public MqttClientId getClientId() {
		return clientId;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassord(String password) {
		this.password = password;
	}

	public int getKeepAlive() {
		return keepAlive;
	}

	public boolean isCleanSession() {
		return cleanSession;
	}

	public Map<String, Integer> getSubscriptions() {
		return Collections.unmodifiableMap(subscriptions);
	}

	public int getMaxTopicsPerSubscribe() {
		return maxTopicsPerSubscribe;
	}

	public void setMaxTopicsPerSubscribe(int maxTopicsPerSubscribe) {
		this.maxTopicsPerSubscribe = maxTopicsPerSubscribe;
	}

	public ClientCallback getCallback() {
		// If callback is null, return a no-op implementation
		return this.callback != null ? this.callback : new ClientCallback() {
			@Override
			public void shutdown() {
				return;
			}

			@Override
			public void messageArrived(MqttServerName mqttServerName, MqttServerUrl mqttServerUrl,
					MqttClientId clientId, String topic, MqttMessage message) {
			}

			@Override
			public void connectionLost(MqttServerName mqttServerName, MqttServerUrl mqttServerUrl,
					MqttClientId clientId, Throwable cause) {
			}

			@Override
			public void connectComplete(boolean reconnect, MqttServerName mqttServerName, MqttServerUrl mqttServerUrl,
					MqttClientId clientId) {
			}
		};
	}

	public void setAutoReconnect(boolean autoReconnect) {
		this.autoReconnect = autoReconnect;
	}

	public boolean getAutoReconnect() {
		return autoReconnect;
	}

	public String getLwtTopic() {
		return lwtTopic;
	}

	public void setLwtRetain(boolean retain) {
		this.lwtRetain = retain;
	}

	public boolean getLwtRetain() {
		return lwtRetain;
	}

	public Long getLastStateDeathPayloadTimestamp() {
		return lastStateDeathPayloadTimestamp;
	}

	public boolean isConnected() {
		if (client != null) {
			return client.isConnected();
		} else {
			return false;
		}
	}

	public boolean isConnectedAndResubscribed() {
		if (client != null) {
			return client.isConnected() && resubscribed;
		} else {
			return false;
		}
	}

	public long getConnectDuration() throws TahuException {
		if (getConnectTime() != null) {
			Date now = new Date();
			return now.getTime() - getConnectTime().getTime();
		} else if (getDisconnectTime() != null) {
			Date now = new Date();
			return -(now.getTime() - getDisconnectTime().getTime());
		} else {
			throw new TahuException(TahuErrorCode.INTERNAL_ERROR, "Connect time is unknown");
		}
	}

	/**
	 * Returns the availability as a percentage, calculated by uptime/(uptime+downtime).
	 * 
	 * @return a double representing the percentage of availability
	 * @throws TahuException
	 */
	public double getAvailability() throws TahuException {
		if (getConnectTime() != null) {
			Date now = new Date();
			totalUptime = totalUptime + (now.getTime() - getConnectTime().getTime());
		}
		if (getDisconnectTime() != null) {
			Date now = new Date();
			totalDowntime = totalDowntime + (now.getTime() - getDisconnectTime().getTime());
		}

		if ((totalUptime + totalDowntime == 0)) {
			throw new TahuException(TahuErrorCode.INTERNAL_ERROR, "Connect time is unknown");
		}

		return (totalUptime / (totalUptime + totalDowntime)) * 100.0;
	}

	public void resetAvailability() {
		totalUptime = 0;
		totalDowntime = 0;
	}

	public int getBufferedMessageCount() {
		try {
			return client.getBufferedMessageCount();
		} catch (Exception e) {
			logger.debug("Failed to get the MQTT Client Buffered Message Count", e);
			return 0;
		}
	}

	public int getInFlightMessageCount() {
		try {
			return client.getInFlightMessageCount();
		} catch (Exception e) {
			logger.debug("Failed to get the MQTT Client In-Flight Message Count", e);
			return 0;
		}
	}

	public Debug getDebug() {
		try {
			return client.getDebug();
		} catch (Exception e) {
			logger.debug("Failed to get the MQTT Client Debug info", e);
			return null;
		}
	}

	public Properties getClientCommsDebug() {
		if (client != null) {
			return client.getClientCommsDebug();
		} else {
			return null;
		}
	}

	public Properties getClientStateDebug() {
		if (client != null) {
			return client.getClientStateDebug();
		} else {
			return null;
		}
	}

	public Properties getConOptions() {
		if (client != null) {
			return client.getConOptions();
		} else {
			return null;
		}
	}

	public Date getClientInitDateTime() {
		return client.getClientInitDateTime();
	}

	/**
	 * Returns a {@link Date} instance representing the online date.
	 * 
	 * @return the online date.
	 */
	public Date getOnlineDateTime() {
		return this.onlineDate;
	}

	/**
	 * Renews the online date.
	 */
	public void renewOnlineDate() {
		this.onlineDate = new Date();
	}

	/**
	 * Returns a {@link Date} instance representing the offline date.
	 * 
	 * @return the offline date.
	 */
	public Date getOfflineDateTime() {
		return this.offlineDate;
	}

	/**
	 * Renews the offline date.
	 */
	public void renewOfflineDate() {
		this.offlineDate = new Date();
	}

	/**
	 * Publishes a message, or buffers it in FIFO order if it cannot be published immediately.
	 *
	 * A QoS > 0 message needs an in-flight permit. Rather than waiting for one - which is what deadlocked this client
	 * against {@link #deliveryComplete(IMqttDeliveryToken)} - a message that cannot get a permit immediately is
	 * appended to the publish buffer and sent later by the drain thread.
	 *
	 * Once the buffer is non-empty every subsequent QoS > 0 message is appended to it until it drains, so buffered
	 * messages are sent in submission order while sends keep succeeding. That ordering is BEST EFFORT: it is not held
	 * across a failed send, and MQTT does not order messages across an in-flight window greater than one in any case.
	 * Callers that need a strict sequence must carry it in the payload.
	 *
	 * QoS 0 is never buffered - it takes no permit and is always sent immediately, so it may overtake queued QoS > 0
	 * messages while the in-flight window is exhausted.
	 *
	 * @return the delivery token if the message was published inline, or <b>null</b> if it was buffered. A null return
	 *         means the message is queued, not lost - callers must not treat it as a failure.
	 * @throws TahuException if the client is not usable, the publish itself failed, or the buffer is full
	 */
	public IMqttDeliveryToken publish(String topic, byte[] payload, int qos, boolean retained) throws TahuException {
		try {
			if (client == null) {
				throw new TahuException(TahuErrorCode.INTERNAL_ERROR,
						"MQTT client: " + clientId.getMqttClientId() + " is null");
			} else if (client.isConnected()) {
				return publishOrBuffer(topic, payload, qos, retained);
			} else {
				throw new TahuException(TahuErrorCode.INTERNAL_ERROR,
						"MQTT client: " + clientId.getMqttClientId() + " is not connected");
			}
		} catch (TahuException e) {
			/*
			 * No InterruptedException handler here, deliberately. 3.x added one to distinguish an interrupt that is
			 * part of an intentional disconnect from one that silently dropped a message - but that only mattered while
			 * publish() could block. The interruptible semaphore.acquire() is gone, replaced by a non-blocking
			 * tryAcquire() plus the publish buffer, so nothing in the try above can throw InterruptedException and Java
			 * would reject a catch for it. The failure mode that change guarded against cannot arise on this path.
			 *
			 * Its underlying concern does still apply, in a new form: publish() returns null when a message is
			 * buffered, and callers that treat null as failure - or ignore it - can lose a BIRTH or an LWT. That is
			 * tracked separately, not papered over here.
			 */
			throw e;
		} catch (Exception e) {
			throw new TahuException(TahuErrorCode.INTERNAL_ERROR, e);
		}
	}

	/**
	 * Decides - atomically with respect to the buffer - whether a message is published now or queued.
	 *
	 * The whole decision runs under {@link #publishOrderLock} so that a message cannot overtake one already queued -
	 * except across a failed buffered send, where the lock is released between the failure and the re-queue. It
	 * NEVER waits for a permit while holding a lock: the inline path uses a non-blocking {@link Semaphore#tryAcquire()}
	 * and falls back to buffering. That is what keeps {@link #deliveryComplete(IMqttDeliveryToken)} - which needs only
	 * {@link #messageLock} - able to return permits at all times.
	 *
	 * Lock order is always publishOrderLock -> messageLock. deliveryComplete() takes messageLock only, so there is no
	 * cycle.
	 */
	private IMqttDeliveryToken publishOrBuffer(String topic, byte[] payload, int qos, boolean retained)
			throws TahuException {
		/*
		 * A QoS 0 message can go out ahead of buffered QoS > 0 ones during a stall. That is deliberate: MQTT only
		 * orders within a QoS level, and holding live fire-and-forget data behind a stalled acknowledged queue would
		 * make it stale for no delivery benefit.
		 *
		 * Deliberately OUTSIDE publishOrderLock, and it has to be for that to hold at all. The drain thread
		 * holds that monitor across client.publish() for every buffered message, and Java monitors are not fair, so a
		 * QoS 0 publisher that took it would queue behind the drain rather than slot in between its iterations -
		 * measured at 500-900ms against a five message backlog, and it scales with buffer depth. This path touches
		 * neither publishBuffer nor semaphore, only the volatile client, so it needs no lock at all. Every Sparkplug
		 * publish is QoS 0, and EdgeClient publishes while holding its own clientLock, so a QoS 0 publisher stalled
		 * here stalls the whole Sparkplug path with it - sequence number allocation included.
		 */
		if (qos == 0) {
			// Read once: disconnect() nulls the field without holding publishOrderLock, and holding it here would
			// not have helped anyway.
			final TahuMqttAsyncClient publishClient = client;
			try {
				logger.debug("{}: Publishing with QoS0 on {}, Payload size = {}", getClientId(), topic, payload.length);
				return publishClient.publish(topic, payload, qos, retained);
			} catch (Throwable t) {
				throw new TahuException(TahuErrorCode.INTERNAL_ERROR, t);
			}
		}

		synchronized (publishOrderLock) {
			// Order preservation among QoS > 0: anything queued means this message queues behind it
			if (!publishBuffer.isEmpty()) {
				bufferPublish(topic, payload, qos, retained, "buffer is draining");
				return null;
			}

			final Semaphore permitHolder = semaphore;
			if (permitHolder == null) {
				throw new TahuException(TahuErrorCode.INTERNAL_ERROR, "MQTT client: " + clientId.getMqttClientId()
						+ " has no in-flight window - it has never connected");
			}

			// Non-blocking on purpose - never wait for a permit while holding a lock
			if (!permitHolder.tryAcquire()) {
				bufferPublish(topic, payload, qos, retained, "no in-flight permit available");
				return null;
			}

			logger.trace("{}: Took permit in publish - available permits remaining: {}", getClientId(),
					permitHolder.availablePermits());
			return publishWithPermit(permitHolder, topic, payload, qos, retained);
		}
	}

	/**
	 * Publishes a message for which an in-flight permit has already been taken.
	 *
	 * Ownership of the permit transfers to this method: on success it belongs to
	 * {@link #deliveryComplete(IMqttDeliveryToken)}, and on failure it is released here. Callers must not release it
	 * themselves.
	 *
	 * Must be called holding {@link #publishOrderLock}.
	 */
	private IMqttDeliveryToken publishWithPermit(Semaphore permitHolder, String topic, byte[] payload, int qos,
			boolean retained) throws TahuException {
		// Reentrant - every caller already holds this. Taken explicitly so publish ordering cannot be bypassed.
		synchronized (publishOrderLock) {
			boolean handedOff = false;
			try {
				synchronized (messageLock) {
					logger.debug("{}: Publishing with QoS{} on {}, Payload size = {}", getClientId(), qos, topic,
							payload.length);
					IMqttDeliveryToken token = client.publish(topic, payload, qos, retained);
					lockedMessageSet.add(token.getMessageId());
					handedOff = true;
					return token;
				}
			} catch (Throwable t) {
				logger.error("{}: Failed to publish on {} - releasing permit - available permits: {}", getClientId(),
						topic, permitHolder.availablePermits(), t);
				throw new TahuException(TahuErrorCode.INTERNAL_ERROR, t);
			} finally {
				// Release only if the message never reached the MQTT client. Once it has, the permit belongs to
				// deliveryComplete(). If lockedMessageSet.add() is what failed, releasing here is still correct -
				// deliveryComplete()'s remove() will return false and it will not release a second time.
				if (!handedOff) {
					permitHolder.release();
				}
			}
		}
	}

	/*
	 * The only four ways publishBuffer may be mutated. Every one of them keeps publishBufferBytes in step, which is
	 * why they exist: an add or a remove that forgot the total would leave the client refusing publishes against a
	 * buffer that had already drained. All must be called holding publishOrderLock.
	 */
	private void bufferAddLast(BufferedPublish message) {
		publishBuffer.addLast(message);
		publishBufferBytes += message.payload.length;
	}

	private void bufferAddFirst(BufferedPublish message) {
		publishBuffer.addFirst(message);
		publishBufferBytes += message.payload.length;
	}

	private BufferedPublish bufferRemoveFirst() {
		BufferedPublish message = publishBuffer.removeFirst();
		publishBufferBytes -= message.payload.length;
		return message;
	}

	private void bufferClear() {
		publishBuffer.clear();
		publishBufferBytes = 0;
	}

	/**
	 * Appends a message to the publish buffer. Must be called holding {@link #publishOrderLock}.
	 *
	 * The buffer is bounded. An unbounded one would simply move the original failure mode from thread exhaustion to
	 * heap exhaustion under a sustained server stall, so at capacity the newest message is rejected and the caller is
	 * told - letting it fall back to its own store-and-forward rather than silently losing data.
	 *
	 * INVARIANT: only QoS > 0 messages are ever buffered. The drain thread relies on this - it takes a permit before
	 * dequeuing, so a buffered QoS 0 message (which needs no permit and is never ACKed) would stall the buffer for as
	 * long as the in-flight window stayed exhausted.
	 * TahuClientPublishBufferTest#bufferNeverHoldsQos0Messages guards it.
	 *
	 * A message is also rejected when no drain thread owns the buffer, for the same reason as a full one: nothing
	 * would ever send it. That state is not reachable during a live session - connect() starts the drain before the
	 * Paho client exists, and publish() cannot get here with no client - so it means the disconnect is already
	 * tearing this client down. Accepting there would return null - telling the caller the message is queued rather
	 * than lost - for a message certain to be discarded by the disconnect that is already running.
	 */
	private void bufferPublish(String topic, byte[] payload, int qos, boolean retained, String reason)
			throws TahuException {
		// Reentrant - every caller already holds this. Taken explicitly so the buffer can never be mutated without it.
		synchronized (publishOrderLock) {
			if (publishBufferDrain == null) {
				// A refusal, not a discard: the caller is told in time to store the message itself.
				publishBufferRejectedMessageCount++;
				throw new TahuException(TahuErrorCode.INTERNAL_ERROR,
						"MQTT client: " + clientId.getMqttClientId() + " is disconnecting - no publish buffer drain "
								+ "thread to send a buffered message, rejecting publish on " + topic);
			}

			if (publishBuffer.size() >= getPublishBufferCapacity()) {
				publishBufferRejectedMessageCount++;
				throw new TahuException(TahuErrorCode.INTERNAL_ERROR,
						"MQTT client: " + clientId.getMqttClientId() + " publish buffer is full ("
								+ publishBuffer.size() + "/" + getPublishBufferCapacity()
								+ ") - rejecting publish on " + topic);
			}

			// Checked before the budget so the message names the payload rather than the buffer's current state -
			// a message this size will never fit, whatever else is queued, and saying so is more use than "full".
			if (payload.length > getPublishBufferByteCapacity()) {
				publishBufferRejectedMessageCount++;
				throw new TahuException(TahuErrorCode.INTERNAL_ERROR,
						"MQTT client: " + clientId.getMqttClientId() + " payload of " + payload.length
								+ " bytes exceeds the whole publish buffer budget of " + getPublishBufferByteCapacity()
								+ " bytes - rejecting publish on " + topic);
			}

			if (publishBufferBytes + payload.length > getPublishBufferByteCapacity()) {
				publishBufferRejectedMessageCount++;
				throw new TahuException(TahuErrorCode.INTERNAL_ERROR,
						"MQTT client: " + clientId.getMqttClientId() + " publish buffer is at its byte capacity ("
								+ publishBufferBytes + "/" + getPublishBufferByteCapacity() + " bytes, "
								+ publishBuffer.size() + " messages) - rejecting publish on " + topic);
			}

			bufferAddLast(new BufferedPublish(topic, payload, qos, retained));
			long now = System.currentTimeMillis();
			if (publishBuffer.size() == 1 && now - lastBufferingWarnTime >= BUFFERING_WARN_INTERVAL) {
				lastBufferingWarnTime = now;
				logger.warn("{}: Buffering publish on {} - {} - subsequent messages will queue behind it",
						getClientId(), topic, reason);
			} else {
				logger.debug("{}: Buffering publish on {} - {} - buffer depth is now {}", getClientId(), topic, reason,
						publishBuffer.size());
			}
			publishOrderLock.notifyAll();
		}
	}

	/**
	 * @return the number of messages currently waiting in the publish buffer. 0 in normal operation - a sustained
	 *         non-zero value means the MQTT server is not acknowledging fast enough.
	 */
	public int getPublishBufferDepth() {
		synchronized (publishOrderLock) {
			return publishBuffer.size();
		}
	}

	/**
	 * @return the number of publishes refused because the publish buffer was already at capacity, for the life of
	 *         this client. Any non-zero value means the MQTT server stalled for longer than the buffer could absorb.
	 *         These are refusals rather than losses from the caller's point of view:
	 *         {@link #publish(String, byte[], int, boolean)} throws, so the caller knows immediately and can fall
	 *         back to its own store and forward. For messages this client accepted and then could not send, see
	 *         {@link #getPublishBufferDiscardedMessageCount()}.
	 */
	public long getPublishBufferRejectedMessageCount() {
		synchronized (publishOrderLock) {
			return publishBufferRejectedMessageCount;
		}
	}

	/**
	 * @return the number of buffered publishes this client accepted and then did not send, for the life of this
	 *         client. Every one of them is lost data the caller could not see coming, because
	 *         {@link #publish(String, byte[], int, boolean)} returned null - queued rather than lost - before the
	 *         loss happened. Two routes reach it:
	 *         <ul>
	 *         <li>a buffered message dropped once its retry window is spent - see requeueOrDrop();</li>
	 *         <li>the whole buffer discarded at disconnect, counted by the number of entries cleared. Since
	 *         connect() disconnects first, an ordinary reconnect takes this route: a queued message must not be
	 *         replayed onto the next session, so it is discarded here and counted.</li>
	 *         </ul>
	 *         There is currently no way for the caller to store these rather than lose them.
	 */
	public long getPublishBufferDiscardedMessageCount() {
		synchronized (publishOrderLock) {
			return publishBufferDiscardedMessageCount;
		}
	}

	/*
	 * A message that could not be published immediately and is waiting its turn in the publish buffer.
	 */
	private static class BufferedPublish {
		private final String topic;
		private final byte[] payload;
		private final int qos;
		private final boolean retained;

		/*
		 * Send attempts made so far. Paho only retries messages it has accepted; a throw from its publish() means the
		 * message never entered its outbound queue or persistence, so nothing else will resend it.
		 */
		private int attempts;

		/* When this message first failed to send, so the retry budget can be measured in wall clock. */
		private long firstFailureTime;

		private BufferedPublish(String topic, byte[] payload, int qos, boolean retained) {
			this.topic = topic;
			this.payload = payload;
			this.qos = qos;
			this.retained = retained;
		}
	}

	private void startPublishBufferDrainThread() {
		synchronized (publishOrderLock) {
			if (publishBufferDrain != null) {
				return;
			}

			/*
			 * A new session never inherits queued messages. disconnect() is supposed to have discarded them, and
			 * this does not trust that it did - any teardown path that skipped it, or anything that reached the
			 * buffer after it ran, would otherwise be replayed here on a connection it was not addressed to.
			 */
			if (!publishBuffer.isEmpty()) {
				publishBufferDiscardedMessageCount += publishBuffer.size();
				logger.warn("{}: Discarding {} buffered publishes left from a previous session", getClientId(),
						publishBuffer.size());
				bufferClear();
			}

			publishBufferDrain = new PublishBufferDrain();
			publishBufferDrainThread = new Thread(publishBufferDrain,
					"TahuPublishBufferDrain-" + getClientId().getMqttClientId());
			publishBufferDrainThread.setDaemon(true);
			publishBufferDrainThread.start();
			logger.debug("{}: Started the publish buffer drain thread", getClientId());
		}
	}

	/*
	 * Waits, bounded, for the client to be able to publish inline again - a free in-flight permit and an empty
	 * buffer, the two conditions publishOrBuffer() would otherwise buffer on.
	 *
	 * Must be called with clientLock released. Both conditions clear only when deliveryComplete() returns a permit,
	 * and deliveryComplete() runs on Paho's single callback thread - so a caller that blocks that thread, or that
	 * holds a lock that thread needs, waits for something only it can deliver. It returns the moment the window
	 * opens, and gives up early if the client goes away, since neither condition can clear after that.
	 */
	private void awaitPublishableWindow(long timeoutMillis) {
		long deadline = System.currentTimeMillis() + timeoutMillis;
		while (System.currentTimeMillis() < deadline) {
			if (getAvailablePublishPermits() > 0 && getPublishBufferDepth() == 0) {
				return;
			}
			if (semaphore == null || client == null || !client.isConnected()) {
				return;
			}
			try {
				Thread.sleep(BIRTH_BACKPRESSURE_POLL);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
	}

	/**
	 * Waits, bounded, for the publish buffer to drain.
	 *
	 * Called from disconnect() BEFORE the LWT is published, so the LWT can go out inline at its configured QoS and be
	 * acknowledged rather than queued behind the backlog into a buffer that is about to be torn down.
	 *
	 * Deliberately NOT called from publishLwt(). That method holds {@link #lwtDeliveryLock}, and
	 * {@link #deliveryComplete(IMqttDeliveryToken)} needs the same monitor before it can return an in-flight permit -
	 * so waiting there would block the very mechanism that drains the buffer, and the wait could never succeed. Here
	 * the calling thread holds only clientLock, which neither the drain thread nor deliveryComplete() takes.
	 */
	private void awaitPublishBufferDrained(long timeoutMillis) {
		int depth = getPublishBufferDepth();
		if (depth == 0) {
			return;
		}

		logger.info("{}: Waiting up to {}ms for {} buffered publishes to drain before publishing the LWT",
				getClientId(), timeoutMillis, depth);
		long deadline = System.currentTimeMillis() + timeoutMillis;
		long noProgressTimeout = Math.max(DRAIN_NO_PROGRESS_FLOOR, timeoutMillis / DRAIN_NO_PROGRESS_FRACTION);
		int lastDepth = depth;
		int lastPermits = getAvailablePublishPermits();
		long lastProgress = System.currentTimeMillis();
		while (System.currentTimeMillis() < deadline) {
			if (getPublishBufferDepth() == 0) {
				logger.debug("{}: Publish buffer drained before the LWT", getClientId());
				return;
			}

			// The same conditions the drain refuses to publish under - waiting past them cannot achieve anything,
			// and every millisecond here is spent holding clientLock
			if (semaphore == null || client == null || !client.isConnected()) {
				logger.debug("{}: Stopped waiting for the publish buffer - the client can no longer publish",
						getClientId());
				return;
			}

			/*
			 * Give up when nothing is moving, rather than when the waiter happens to be a particular thread.
			 *
			 * The wait is starved whenever the callback thread cannot reach deliveryComplete(), and that is not only
			 * when this thread IS the callback thread. Paho dispatches deliveryComplete, messageArrived and
			 * connectComplete from one queue on one thread, so a callback ahead of deliveryComplete that blocks on
			 * any lock this waiter holds starves it just as completely - and that lock can belong to the embedder,
			 * where nothing here can see it. Testing progress instead covers every such topology, including the ones
			 * we have not thought of.
			 *
			 * A draining buffer moves in steps: a permit returns, depth falls. Neither changing for this long means
			 * nothing is servicing it.
			 */
			int currentDepth = getPublishBufferDepth();
			int currentPermits = getAvailablePublishPermits();
			if (currentDepth != lastDepth || currentPermits != lastPermits) {
				lastDepth = currentDepth;
				lastPermits = currentPermits;
				lastProgress = System.currentTimeMillis();
			} else if (System.currentTimeMillis() - lastProgress >= noProgressTimeout) {
				logger.debug("{}: Stopped waiting for the publish buffer - no progress for {}ms, so nothing is "
						+ "servicing it", getClientId(), noProgressTimeout);
				return;
			}
			try {
				Thread.sleep(DRAIN_WAIT_POLL);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
		logger.warn("{}: Publish buffer still holds {} messages after {}ms - the LWT will fall back to QoS 0, and "
				+ "anything still queued will be discarded", getClientId(), getPublishBufferDepth(), timeoutMillis);
	}

	/*
	 * Stops the drain thread without letting a failure abort a disconnect that is already in progress.
	 *
	 * Called from both arms of disconnect(): a client that was never created still has a drain thread to stop,
	 * because connect() starts it before the Paho client is built.
	 */
	private void shutdownPublishBufferDrainThreadQuietly() {
		try {
			shutdownPublishBufferDrainThread();
		} catch (Exception e) {
			logger.error("{}: Failed to shutdown publish buffer drain thread", getClientId());
		}
	}

	private void shutdownPublishBufferDrainThread() {
		final Thread drainThread;
		synchronized (publishOrderLock) {
			/*
			 * Discard FIRST, and unconditionally - above the drain == null guard, not below it.
			 *
			 * A message can reach the buffer after the drain has already been stopped: publish() admits anything
			 * while client.isConnected() is true, which it remains from here until disconnectForcibly(), including
			 * across the deliberate sleep in disconnect(). Nothing owns those entries. Discarding them below the
			 * guard meant the next disconnect - connect() calls one before every attempt, and by then the drain is
			 * already null - returned without clearing, so connect()'s new drain thread found a non-empty buffer and
			 * flushed the previous session's messages onto the new connection. For a death certificate that is a
			 * retained "offline" arriving on a live session, ahead of the BIRTH queued behind it.
			 */
			if (!publishBuffer.isEmpty()) {
				// Counted, not just logged: publish() told these callers the message was queued, not lost, so this
				// is the only place the loss can be reported to them - see getPublishBufferDiscardedMessageCount().
				publishBufferDiscardedMessageCount += publishBuffer.size();
				logger.warn("{}: Discarding {} buffered publishes ({} bytes) on shutdown", getClientId(),
						publishBuffer.size(), publishBufferBytes);
				bufferClear();
			}
			publishOrderLock.notifyAll();

			if (publishBufferDrain == null) {
				return;
			}
			publishBufferDrain.setKeepRunning(false);
			drainThread = publishBufferDrainThread;
			publishBufferDrain = null;
			publishBufferDrainThread = null;
			// Again, now that keepRunning is false, so a drain parked in wait() sees the flag rather than relying on
			// the interrupt below to be what stops it.
			publishOrderLock.notifyAll();
		}
		if (drainThread != null) {
			drainThread.interrupt();
		}
	}

	/**
	 * Puts a failed send back at the head of the buffer, or drops it once its retry budget is spent.
	 *
	 * Paho does not rescue this message. Its QoS 1/2 redelivery only covers messages already accepted into its
	 * outbound queue and persistence, and a throw from its publish() means this one never got that far. If it is not
	 * re-queued here it is gone.
	 *
	 * The budget is a window of wall clock, not a count of attempts. The realistic failures - REASON_CODE_MAX_INFLIGHT
	 * (32202) when Tahu's permit count has drifted above Paho's window, or REASON_CODE_CLIENT_NOT_CONNECTED (32104) if
	 * the connection drops between the check and the send - are transient and clear on their own, which a count alone
	 * gave them no time to do.
	 *
	 * NOTE: the caller has already left the publishOrderLock block by the time this runs, so a concurrent publisher can
	 * slip in ahead of the message being re-queued. That reordering is accepted rather than prevented, so
	 * "back at the head" means ahead of everything still queued, not ahead of everything published after the failure.
	 *
	 * The drain is passed in so this can refuse to re-queue into a buffer it no longer owns. publishWithPermit()
	 * throws from inside a synchronized (publishOrderLock) block that the caller's catch sits outside of, so the
	 * monitor is released and re-acquired between the failed send and the re-queue - and a disconnect can win it in
	 * that gap, discard the buffer, retire this drain and let connect() start another. Putting the message back then
	 * hands a dead session's publish to the next session's drain, at the head of its queue, which is the replay
	 * c70d86f set out to make impossible. A null check on the field is not enough: by that point it is non-null and
	 * pointing at a different drain.
	 *
	 * @return how long the caller should wait before the next attempt, or 0 if the message was dropped. The caller
	 *         must do the waiting: this method holds publishOrderLock, and sleeping here would block every publisher
	 *         on this client for the duration of the backoff.
	 */
	private long requeueOrDrop(BufferedPublish attempted, Throwable cause, PublishBufferDrain drain) {
		if (attempted == null) {
			return 0;
		}

		synchronized (publishOrderLock) {
			if (drain != publishBufferDrain || !drain.keepRunning) {
				// Counted as a discard, not a rejection: publish() returned null for this message long ago
				publishBufferDiscardedMessageCount++;
				logger.warn("{}: Discarding a buffered publish on {} - its session ended while the send was being "
						+ "retried, and it must not be replayed on the next one", getClientId(), attempted.topic,
						cause);
				return 0;
			}

			long now = System.currentTimeMillis();
			if (attempted.firstFailureTime == 0) {
				attempted.firstFailureTime = now;
			}
			attempted.attempts++;
			long elapsed = now - attempted.firstFailureTime;

			if (elapsed < MAX_BUFFERED_PUBLISH_RETRY_WINDOW && attempted.attempts < MAX_BUFFERED_PUBLISH_ATTEMPTS) {
				bufferAddFirst(attempted);
				// Every other write to the buffer notifies; this one did not, so a re-queued message could sit
				// unnoticed until some later publish happened to wake the drain
				publishOrderLock.notifyAll();
				long delay = Math.min(PUBLISH_BUFFER_POLL_INTERVAL * attempted.attempts,
						MAX_BUFFERED_PUBLISH_RETRY_DELAY);
				logger.warn("{}: Failed to send buffered publish on {} - re-queued at head, attempt {}, {}ms into a "
						+ "{}ms window, retrying in {}ms", getClientId(), attempted.topic, attempted.attempts, elapsed,
						MAX_BUFFERED_PUBLISH_RETRY_WINDOW, delay, cause);
				return delay;
			}

			// Counted, not just logged: publish() returned null for this message, so the caller was told it was
			// queued and has no other way to learn that it was not sent.
			publishBufferDiscardedMessageCount++;
			logger.error("{}: Dropping buffered publish on {} after {} attempts over {}ms", getClientId(),
					attempted.topic, attempted.attempts, elapsed, cause);
			return 0;
		}
	}

	/*
	 * Drains the publish buffer head first as in-flight permits become available. FIFO on the happy path only -
	 * a failed send can be overtaken while it is re-queued.
	 *
	 * Permits are acquired WITHOUT holding publishOrderLock - only the dequeue-and-publish step takes it - so this
	 * thread can never block a caller or deliveryComplete().
	 *
	 * Relies on the buffer holding only QoS > 0 messages - see bufferPublish().
	 */
	private class PublishBufferDrain implements Runnable {

		private volatile boolean keepRunning = true;

		private void setKeepRunning(boolean keepRunning) {
			this.keepRunning = keepRunning;
		}

		@Override
		public void run() {
			while (keepRunning) {
				try {
					synchronized (publishOrderLock) {
						while (keepRunning && publishBuffer.isEmpty()) {
							publishOrderLock.wait();
						}
						if (!keepRunning) {
							return;
						}
					}

					if (semaphore == null || client == null || !client.isConnected()) {
						// Nothing can be sent yet - leave the buffer intact and re-check shortly
						Thread.sleep(PUBLISH_BUFFER_POLL_INTERVAL);
						continue;
					}

					// Every buffered message is QoS > 0 - publishOrBuffer() sends QoS 0 straight to the MQTT
					// client and never queues it - so a permit is always required here.
					final Semaphore permitHolder = semaphore;
					if (!permitHolder.tryAcquire(PUBLISH_BUFFER_POLL_INTERVAL, TimeUnit.MILLISECONDS)) {
						continue;
					}

					boolean permitOwnershipTransferred = false;
					BufferedPublish attempted = null;
					long retryDelay = 0;
					try {
						synchronized (publishOrderLock) {
							BufferedPublish head = publishBuffer.peekFirst();
							if (head == null) {
								continue;
							}

							bufferRemoveFirst();
							attempted = head;
							permitOwnershipTransferred = true;
							publishWithPermit(permitHolder, head.topic, head.payload, head.qos, head.retained);

							attempted = null;
							if (publishBuffer.isEmpty()) {
								// DEBUG, not INFO: this is the other half of the buffering pair above, and an
								// oscillating buffer would log it just as often. Publish Buffer Depth is where
								// the state is meant to be read.
								logger.debug("{}: Publish buffer drained", getClientId());
							}
						}
					} catch (Throwable t) {
						/*
						 * The backoff is taken here rather than inside requeueOrDrop(), which holds publishOrderLock.
						 * It matters that something waits: after a failure every delay point in this loop is skipped -
						 * the buffer is non-empty so wait() does not fire, the client still reports connected so the
						 * poll sleep is not taken, and tryAcquire() returns at once because the permit the failed send
						 * just released is free again. Without this the whole retry window is spent in microseconds.
						 */
						retryDelay = requeueOrDrop(attempted, t, this);
					} finally {
						if (!permitOwnershipTransferred) {
							permitHolder.release();
						}
					}

					if (retryDelay > 0) {
						Thread.sleep(retryDelay);
					}
				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
					return;
				} catch (Throwable t) {
					logger.error("{}: Publish buffer drain thread error", getClientId(), t);
				}
			}
		}
	}

	public void asyncPublish(String topic, byte[] payload, int qos, boolean retained) throws TahuException {
		Thread t = new Thread(new AsyncPublisher(topic, payload, qos, retained, false, 0, 0));
		t.start();
	}

	public void asyncPublish(String topic, byte[] payload, int qos, boolean retained, boolean retry, long retryDelay,
			int numAttempts) throws TahuException {
		Thread t = new Thread(new AsyncPublisher(topic, payload, qos, retained, retry, retryDelay, numAttempts));
		t.start();
	}

	/**
	 * Subscribes to a topic.
	 * 
	 * @param topic the topic.
	 * @param qos the quality of service (0, 1, or 2)
	 * 
	 * @return the granted QoS for the subscription
	 * @throws TahuException
	 */
	public int subscribe(String topic, int qos) throws TahuException {
		synchronized (clientLock) {
			if (client != null) {
				if (client.isConnected()) {
					try {
						logger.debug("{}: server {} - Attempting to subscribe on topic {} with QoS={}", getClientId(),
								getMqttServerName(), topic, qos);
						IMqttToken token = client.subscribe(topic, qos);
						logger.trace("{}: Waiting for subscription on {}", getClientId(), topic);
						token.waitForCompletion();
						logger.trace("{}: Done waiting for subscription on {}", getClientId(), topic);
						subscriptions.put(topic, qos);
						int[] grantedQos = token.getGrantedQos();
						logger.debug("{}: Granted QoS for subcription on {}: {}", getClientId(), topic, grantedQos[0]);
						if (grantedQos != null && grantedQos.length == 1) {
							return grantedQos[0];
						} else {
							String errorMessage = getClientId() + ": server " + getMqttServerName()
									+ " - Failed to subscribe to " + topic;
							logger.error(errorMessage);
							throw new TahuException(TahuErrorCode.NOT_AUTHORIZED, errorMessage);
						}
					} catch (MqttException e) {
						logger.error(getClientId() + ": server " + getMqttServerName() + " - Failed to subscribe to "
								+ topic);
						throw new TahuException(TahuErrorCode.INTERNAL_ERROR, e);
					}
				}
			}
			logger.debug("{}: Not connected and not subscribing to {} - just storing the subscription for now",
					getClientId(), topic);
			subscriptions.put(topic, qos);
			return qos;
		}
	}

	/**
	 * Subscribes to a set of topic.
	 * 
	 * @param topics the topics.
	 * @param qos the quality of service (0, 1, or 2)
	 * 
	 * @return the granted QoS levels for the subscriptions
	 * @throws TahuException
	 */
	public int[] subscribe(String[] topics, int[] qos) throws TahuException {
		synchronized (clientLock) {
			try {
				if (client != null) {
					if (client.isConnected()) {
						logger.debug("{}: Attempting to subscribe on topics {} with QoS={}", getClientId(), topics,
								qos);
						IMqttToken token = client.subscribe(topics, qos);
						logger.trace("{}: Waiting for subscription on {}", getClientId(), Arrays.toString(topics));
						token.waitForCompletion();
						logger.trace("{}: Done waiting for subscription on {}", getClientId(), Arrays.toString(topics));
						int[] grantedQos = token.getGrantedQos();
						if (grantedQos != null && grantedQos.length > 0) {
							for (int i = 0; i < topics.length; i++) {
								if (grantedQos[i] == qos[i]) {
									subscriptions.put(topics[i], qos[i]);
								} else {
									throw new TahuException(TahuErrorCode.NOT_AUTHORIZED,
											"Failed to subscribe to " + topics[i]);
								}
							}

							return grantedQos;
						} else {
							throw new TahuException(TahuErrorCode.NOT_AUTHORIZED, "Failed to subscribe to " + topics);
						}
					}
				}

				for (int i = 0; i < topics.length; i++) {
					subscriptions.put(topics[i], qos[i]);
				}
				logger.debug("{}: Not connected and not subscribing to {} - just storing the subscription for now",
						getClientId(), Arrays.asList(topics));
				return qos;
			} catch (Exception e) {
				throw new TahuException(TahuErrorCode.INTERNAL_ERROR, e);
			}
		}
	}

	/**
	 * Unsubsribes from a topic.
	 * 
	 * @param topic the topic.
	 * @throws TahuException
	 */
	public void unsubscribe(String topic) throws TahuException {
		synchronized (clientLock) {
			if (client != null) {
				if (client.isConnected()) {
					try {
						logger.debug("{}: {} attempting to unsubscribe on topic {}", getClientId(), mqttServerName,
								topic);
						client.unsubscribe(topic);
					} catch (MqttException e) {
						throw new TahuException(TahuErrorCode.INTERNAL_ERROR, e);
					}
				}
			}
			subscriptions.remove(topic);
		}
	}

	@Override
	public void connectionLost(Throwable cause) {
		logger.debug("{}: MQTT connectionLost() to {} :: {}", getClientId(), getMqttServerName(), getMqttServerUrl());
		if (logger.isTraceEnabled()) {
			if (client != null) {
				client.getDebug().dumpClientDebug();
			}
		}

		// reset the timers if needed
		if (getDisconnectTime() == null) {
			this.clearConnectTime();
			this.renewDisconnectTime();
			this.renewOfflineDate();
		}

		// Reset re-subscribed flag
		resubscribed = false;

		if (cause != null) {
			// We don't need to see all of the connection lost callbacks for clients
			logger.debug("{}: Connection lost due to {}", getClientId(), cause.getMessage(), cause);
		}

		// Trigger the connection lost event on the callback client
		getCallback().connectionLost(getMqttServerName(), getMqttServerUrl(), getClientId(), cause);
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
		try {
			synchronized (lwtDeliveryLock) {
				if (lwtDeliveryToken != null && lwtDeliveryToken.getMessageId() == token.getMessageId()) {
					logger.info("{}: LWT Delivery complete for {}", getClientId(), token.getMessageId());
					lwtDeliveryToken = null;
				} else {
					logger.debug("{}: Delivery complete for {}", getClientId(), token.getMessageId());
				}
			}
		} catch (Throwable t) {
			logger.error("Failed to handle delivery complete for {}", token);
		} finally {
			synchronized (messageLock) {
				if (lockedMessageSet.remove(token.getMessageId())) {
					logger.trace("{}: Releasing permit - Available permits: {}", getClientId(),
							semaphore.availablePermits());
					semaphore.release();
				}
			}
		}
	}

	@Override
	public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
		logger.debug("{}: MQTT message arrived on topic {}", getClientId(), topic);
		numMesgsArrived++;
		getCallback().messageArrived(getMqttServerName(), getMqttServerUrl(), getClientId(), topic, mqttMessage);
	}

	/**
	 * Attempt to connect the TahuClient
	 */
	public void connect() {
		connect(false);
	}

	/**
	 * Attempt to connect the TahuClient.
	 *
	 * @param deferredReplay true only for a replay armed by a connect that was refused during a teardown. That is
	 *        the one caller which must be abandoned if the application stopped auto reconnecting while the replay
	 *        was waiting - every other caller may legitimately connect with autoReconnect false.
	 */
	private void connect(boolean deferredReplay) {
		try {
			new URI(mqttServerUrl.getMqttServerUrl());
		} catch (Exception e) {
			logger.error("{}: Invalid MQTT Server URL: {}", getClientId(), mqttServerUrl.getMqttServerUrl());
			return;
		}

		/*
		 * Refused, not queued, while a disconnect is in flight. The teardown clears the client field before closing
		 * the Paho client, so between those two points isConnected() reports false while the old socket is still
		 * open - and a poll loop that reconnects on that would bring up a second session with the same client ID.
		 * The MQTT server then disconnects the older one and publishes its Will, landing a retained offline STATE on
		 * a session that has already announced itself.
		 *
		 * A refusal rather than a wait, deliberately. Callers that reconnect on a poll retry on their next tick,
		 * which is the ordering the lock used to give; waiting here would put this thread back into the cycle the
		 * teardown was restructured to break.
		 */
		logger.debug("{}: Starting new connect, autoReconnect: {}", getClientId(), autoReconnect);

		/*
		 * The previous session is detached under clientLock and closed after it is released, rather than by calling
		 * disconnect() from inside the lock. Monitors are reentrant, so a nested disconnect() would exit its own
		 * synchronized block with the lock still held by this frame - and closeDetachedSession() must not run that
		 * way. See the comment on that method.
		 */
		DetachedSession detached = null;
		boolean claimed = false;
		synchronized (clientLock) {
			logger.debug("{}: Got lock for new connect", getClientId());

			/*
			 * Deferred rather than dropped, and recorded under the same lock the gate is read under. A caller whose
			 * only reconnect trigger is one-shot - TahuHostCallback's connectionLost is exactly that, and
			 * HostApplication has no run loop behind it - would otherwise lose its single attempt and never return.
			 *
			 * The teardown replays it when its last claim is released, so there is nothing to poll and no deadline
			 * to expire. A teardown that never finishes leaves the client wedged for reasons no reconnect can fix.
			 */
			if (isDisconnectInProgress()) {
				logger.debug("{}: Deferring the connect - a disconnect is in progress", getClientId());
				deferredConnectPending = true;
				return;
			}

			/*
			 * The last chance to abandon a replay, and the only one taken under this lock. runPendingConnect() and
			 * its worker both test autoReconnect, but the worker's test is a few bytecodes ahead of this critical
			 * section - so a shutdown landing in that gap reached a connect with no autoReconnect gate of its own,
			 * because the getAutoReconnect() && state.inProgress() test below stops testing anything once the flag
			 * is false. Here the test and the connect it authorises are in one critical section, so nothing can
			 * interleave between them.
			 *
			 * Scoped to the replay, not unconditional. Connecting with autoReconnect false is a supported single
			 * attempt - ConnectRunnable implements it explicitly, and EdgeClient uses it to try each MQTT server
			 * definition once - so refusing every caller here would stop those clients connecting at all.
			 */
			if (deferredReplay && !autoReconnect) {
				logger.debug("{}: Dropping the deferred connect - the client is no longer auto reconnecting",
						getClientId());
				return;
			}

			try {
				// reset the timers if needed
				if (getDisconnectTime() == null) {
					this.clearConnectTime();
					this.renewDisconnectTime();
				}

				if (getAutoReconnect() && state.inProgress()) {
					logger.debug("{}: Connect attempt already in progress", getClientId());
					return;
				}

				teardownsInFlight.incrementAndGet();
				claimed = true;

				/*
				 * The LWT still publishes, but this does not wait under clientLock for its delivery confirmation.
				 * isLwtDeliveryComplete() polls for keepAlive * 4 quarter seconds - keepAlive seconds, 30 by default
				 * - and the confirmation it waits for arrives on Paho's callback thread, so every other operation on
				 * this client queues behind a reconnect for that long. BirthRecovery blocking there is one of the
				 * threads the wait needs.
				 *
				 * The death certificate is not sacrificed: closeDetachedSession() pays an unconditional second
				 * before it touches the socket, with the lock released, which is the same grace period the wait was
				 * providing. Callers that ask for a graceful disconnect still get the confirmed wait - only this
				 * pre-connect teardown of a session that is being replaced drops it.
				 */
				detached = detachSession(false, true, false);

				/*
				 * Armed here, under the lock that detached the old session, rather than after the close. detachSession
				 * clears it, and between that and re-acquiring the lock the client is null and nothing is in
				 * progress - so a second connect() passed every gate and started its own ConnectRunnable, leaving two
				 * live Paho clients for one client id and no reference to close one of them.
				 */
				state.setInProgress(true);
			} catch (Throwable t) {
				/*
				 * Only if this call actually took a claim. Anything above the increment can throw, and releasing
				 * then drives the count negative - which the correction in releaseTeardownClaim() resets to zero,
				 * clearing a claim another teardown is still holding.
				 */
				logger.error("{}: Error connecting", getClientId(), t);
				if (claimed) {
					releaseTeardownClaim();
					claimed = false;
				}
				return;
			}
		}

		try {
			closeDetachedSession(detached, 0, 0);
		} catch (Exception e) {
			logger.error("{}: Failed to close the previous session - continuing with the connect", getClientId(), e);
		} finally {
			if (claimed) {
				releaseTeardownClaim();
			}
		}

		synchronized (clientLock) {
			try {
				logger.debug("{}: Starting ConnectThread", getClientId());
				connectRunnable = new ConnectRunnable(this);
				connectRunnableThread = new Thread(connectRunnable);
				connectRunnableThread.start();
			} catch (Throwable t) {
				logger.error("{}: Error connecting", getClientId(), t);
			}
		}
	}

	public boolean isDisconnectInProgress() {
		return teardownsInFlight.get() > 0;
	}

	/**
	 * Attempt to disconnect the TahuClient.
	 * 
	 * @param retryConnect true if the client should attempt to reconnect.
	 */
	public void disconnect(long disconnectQuieseTime, long disconnectTimeout, boolean sendDisconnect,
			boolean waitForLwt) throws TahuException {
		this.disconnect(disconnectQuieseTime, disconnectTimeout, sendDisconnect, true, waitForLwt);
	}

	/**
	 * Attempt to disconnect the TahuClient.
	 * 
	 * @param retryConnect true if the client should attempt to reconnect.
	 */
	public void disconnect(long disconnectQuieseTime, long disconnectTimeout, boolean sendDisconnect,
			boolean publishLwt, boolean waitForLwt) throws TahuException {
		disconnectSession(null, disconnectQuieseTime, disconnectTimeout, sendDisconnect, publishLwt, waitForLwt);
	}

	/**
	 * Disconnects, optionally only if the live session is still the one the caller means to tear down.
	 *
	 * Split in two deliberately. Everything that names the session happens under {@link #clientLock}; the Paho close
	 * happens after it is released, because that call can wait without limit for a thread that needs the lock - see
	 * {@link #closeDetachedSession(DetachedSession, long, long)}.
	 *
	 * @param expected the session to tear down, or null to tear down whatever is live
	 * @return true if a session was detached, false if a different one is live
	 */
	private boolean disconnectSession(TahuMqttAsyncClient expected, long disconnectQuieseTime, long disconnectTimeout,
			boolean sendDisconnect, boolean publishLwt, boolean waitForLwt) throws TahuException {
		DetachedSession detached;

		/*
		 * One critical section for the identity check, the claim and the detach. Splitting them let the session
		 * change between the check and the teardown it authorises, so the check governed nothing. Only the Paho
		 * close runs with the lock released, because that is the part that can wait on the callback thread.
		 *
		 * The claim is tracked rather than inferred from reaching the finally. The early return happens before it is
		 * taken, so releasing unconditionally would clear a slot this call never claimed - and anything throwing
		 * between the claim and the close would leak one, wedging the gate shut for the life of the client. Both
		 * directions have been live defects here; the flag is what makes neither reachable.
		 */
		boolean claimed = false;
		try {
			synchronized (clientLock) {
				if (expected != null && client != expected) {
					logger.debug("{}: Not disconnecting - the session to tear down is no longer the live one",
							getClientId());
					return false;
				}

				teardownsInFlight.incrementAndGet();
				claimed = true;
				detached = detachSession(sendDisconnect, publishLwt, waitForLwt);

				// reset the timers if needed
				if (getDisconnectTime() == null) {
					this.clearConnectTime();
					this.renewDisconnectTime();
					this.renewOfflineDate();
				}
			}

			/*
			 * Before the close and outside clientLock. Before, because the point of confirming the death
			 * certificate is to know it left before the socket goes. Outside, because the confirmation arrives on
			 * Paho's callback thread by way of deliveryComplete(), and that thread needs clientLock.
			 *
			 * Still inside the teardown claim, which is what binds the wait to this session: while a claim is held
			 * isDisconnectInProgress() is true, so connect() defers rather than starting a ConnectRunnable, and no
			 * new session can be installed underneath it.
			 */
			if (detached != null && detached.awaitLwtDeliveryToken != null) {
				awaitLwtDelivery(detached.awaitLwtDeliveryToken);
			}

			closeDetachedSession(detached, disconnectQuieseTime, disconnectTimeout);
		} finally {
			if (claimed) {
				releaseTeardownClaim();
			}
		}

		return detached != null;
	}

	/**
	 * Drops any client left over from a previous attempt, before a new one is built.
	 *
	 * Split the same way {@link #detachSession} and {@link #closeDetachedSession} are, and for the same reason: the
	 * field is cleared under {@link #clientLock} and the Paho close runs with the lock released. This runs on
	 * connectRunnableThread, so CommsCallback.stop() takes its uncapped spin wait for Paho's callback thread - and
	 * that thread needs clientLock through three of the four callbacks it dispatches. Holding the lock across the
	 * close here is the same two party deadlock, on the path taken before every reconnect attempt.
	 */
	private void discardClientForReconnect() {
		TahuMqttAsyncClient closing;
		boolean wasConnected;
		synchronized (clientLock) {
			if (client == null) {
				return;
			}

			closing = client;
			wasConnected = client.isConnected();
			if (wasConnected) {
				shutdownConnectionMonitorThread();
			}

			// Cleared here so nothing can reach the client being closed, exactly as detachSession() does
			client = null;
		}

		try {
			if (wasConnected) {
				closing.disconnectForcibly(0, 1, false);
			}
			// closing.setCallback(null);
			closing.close();
		} catch (MqttException e) {
			logger.error("{}: Error while disconnecting client", getClientId(), e);
		}
	}

	/** Releases one teardown claim, and never takes the count below zero if a caller unbalances it. */
	private void releaseTeardownClaim() {
		int remaining = teardownsInFlight.decrementAndGet();
		if (remaining < 0) {
			logger.error("{}: Teardown claim released more often than it was taken - correcting", getClientId());
			teardownsInFlight.set(0);
			remaining = 0;
		}

		if (remaining == 0) {
			synchronized (clientLock) {
				// Re-tested under the lock: another teardown may have claimed while this one was releasing
				if (!isDisconnectInProgress()) {
					runPendingConnect();
				}
			}
		}
	}

	/**
	 * Runs a connect that was refused while a teardown held the gate, once the last teardown has finished.
	 *
	 * Called from {@link #releaseTeardownClaim()} with {@link #clientLock} held, so the pending flag is tested and
	 * cleared in the same critical section that {@link #connect()} sets it in - there is no window in which one
	 * observes the gate and the other observes the flag.
	 *
	 * On a worker rather than inline, for two reasons. The releasing thread can be Paho's callback thread, by way of
	 * EdgeClient disconnecting from messageArrived, and connect() there would hold clientLock across a Paho close
	 * inside the callback dispatch. And releaseTeardownClaim() is reached from connect()'s own finally, so running
	 * connect() inline would recurse through the application callback.
	 */
	private void runPendingConnect() {
		if (!deferredConnectPending) {
			return;
		}

		/*
		 * Refused if the client is being shut down. HostApplication.shutdown() clears autoReconnect and then
		 * disconnects, and a replay after that resurrects a session the application has finished with - republishing
		 * a retained online STATE for a host that is down, which every edge node bound to it would believe.
		 */
		if (!autoReconnect) {
			logger.debug("{}: Dropping the deferred connect - the client is not auto reconnecting", getClientId());
			deferredConnectPending = false;
			return;
		}

		deferredConnectPending = false;

		/*
		 * No dedupe against a worker that is already running. Suppressing a replay on the strength of another one
		 * being alive is how the mechanism this replaced lost connects - the live worker may itself be about to be
		 * refused, and then nothing is left armed. A replay that turns out to be redundant is refused by connect()'s
		 * own in-progress gate at the cost of one short lived thread, and the pending flag bounds how many can be
		 * armed at once.
		 *
		 * autoReconnect is re-read on the worker as well as here, and once more inside connect()'s critical section
		 * - connect(deferredReplay = true). The worker's read is the cheap early-out; the one under the lock is the
		 * one that closes the window, because between the worker's read and that critical section the application
		 * can still shut the client down completely.
		 */
		Thread worker = new Thread(() -> {
			if (!autoReconnect) {
				logger.debug("{}: Dropping the deferred connect - the client stopped auto reconnecting while it was "
						+ "pending", getClientId());
				return;
			}
			connect(true);
		}, "TahuDeferredConnect-" + getClientId().getMqttClientId());
		worker.setDaemon(true);
		worker.start();
	}

	/**
	 * Detaches the live session: stops the connection monitor, the connect runnable and the buffer drain, publishes
	 * the LWT if asked, and clears every field that names the session.
	 *
	 * The caller must hold {@link #clientLock}. Nothing here waits on another thread, and nothing here throws - the
	 * session is detached on every path, so a caller can rely on it being gone once this returns.
	 *
	 * @return the detached session for {@link #closeDetachedSession}, or null if there was no client
	 */
	private DetachedSession detachSession(boolean sendDisconnect, boolean publishLwt, boolean waitForLwt) {
		try {
			shutdownConnectionMonitorThread();
		} catch (Exception e) {
			logger.error("{}: Failed to shutdown connection monitor thread", getClientId());
		}

		try {
			if (connectRunnable != null && connectRunnableThread != null) {
				connectRunnable.stopConnectAttempts();
				connectRunnableThread.interrupt();
			}
		} catch (Exception e) {
			logger.error("{}: Failed to shut down the connect runnable", getClientId());
		}

		/*
		 * A replay armed for a session this is tearing down is not wanted. The intent is cancelled; the worker that
		 * would run it deliberately is not interrupted.
		 *
		 * Interrupting it cancels nothing - connect() never tests the interrupt flag, so a worker past the gate
		 * connects regardless - and on the replay path the thread it interrupts is this one, because the worker runs
		 * connect() and connect() calls this method. Everything left in the teardown from that point is a timed
		 * wait: the pre-LWT drain returns at its first sleep, and closeDetachedSession() skips the unconditional
		 * second that gives the LWT time to reach the server before the socket goes. connect() gives up waiting for
		 * the LWT acknowledgement on the strength of that second, so self-interrupting leaves it with no cover.
		 *
		 * What actually stops a replay landing after a shutdown is this flag plus the autoReconnect re-check the
		 * worker makes immediately before it connects.
		 */
		deferredConnectPending = false;

		try {
			if (client == null) {
				logger.debug("{}: Disconnect: Client is already null", getClientId());

				/*
				 * The drain thread is started by connect() before the Paho client is built, so a disconnect with no
				 * client still has one to stop. Without this it would be left running with no owner, holding its
				 * TahuClient and everything queued in its buffer from garbage collection - the same orphan that any
				 * teardown bypassing disconnect() leaves behind.
				 */
				shutdownPublishBufferDrainThreadQuietly();
				return null;
			}

			boolean sendDisconnectPacket = sendDisconnect;
			IMqttDeliveryToken awaitLwtDeliveryToken = null;
			/*
			 * Give the buffer a bounded chance to drain BEFORE the LWT, so the LWT can go out inline at its
			 * configured QoS and be acknowledged. The drain thread is still running at this point; it is stopped
			 * below, after the LWT.
			 *
			 * Guarded on isConnected() and nothing else. With the socket already down the wait cannot succeed - the
			 * drain declines to publish while the client is disconnected, so the poll ran to the deadline and
			 * publishLwt() then skipped the LWT anyway, measured at 2011ms of dead time on the path connect() takes
			 * before every reconnect attempt.
			 *
			 * Deliberately NOT also scoped to publishLwt. shutdownPublishBufferDrainThreadQuietly() below runs on
			 * every disconnect and discards the buffer unconditionally, so a still-connected client disconnecting
			 * with publishLwt=false would lose an accepted backlog with no chance to flush it - and publish()
			 * returning null told those callers the messages were queued, not lost. The LWT is the reason the wait
			 * happens first, not the reason it happens.
			 *
			 * This still runs under clientLock, and it waits for a permit that only deliveryComplete() returns. It is
			 * not safe by virtue of any lock discipline here - the callback thread can be blocked on a lock this
			 * client knows nothing about, in the embedder - so the wait itself gives up as soon as it sees no
			 * progress rather than relying on who is waiting.
			 */
			if (client.isConnected()) {
				awaitPublishBufferDrained(LWT_DRAIN_TIMEOUT);
			}

			if (publishLwt) {
				/*
				 * A failed LWT publish must not abort the disconnect - the session has to come down either way.
				 *
				 * Published here, under clientLock, and confirmed by disconnectSession() after the lock is
				 * released. isLwtDeliveryComplete() polls for keepAlive * 4 quarter seconds - keepAlive seconds,
				 * 30 by default - for a confirmation that arrives on Paho's callback thread, and that thread needs
				 * clientLock for connectComplete. Waiting here held the lock for the whole of it and blocked one of
				 * the threads the wait depends on. Same split as the Paho close: the part that can wait on the
				 * callback thread runs with the lock released.
				 */
				try {
					/*
					 * The token this call published, not whatever the field holds. The finally below clears
					 * lwtDeliveryToken on every path out, so a token captured from the field was already null by the
					 * time disconnectSession() came to wait on it - and the wait returned on its first comparison,
					 * logging a confirmation it never obtained. Taking the token from the publish keeps it out of
					 * reach of the field's lifecycle entirely, and means a call that published nothing has nothing
					 * to wait on rather than a stale token from an earlier session.
					 */
					IMqttDeliveryToken published = this.publishLwtNow(false);
					if (waitForLwt) {
						awaitLwtDeliveryToken = published;
					}
				} catch (Exception e) {
					logger.error("{}: Failed to publish the LWT during disconnect - continuing", getClientId(), e);
				}

				/*
				 * Last resort. No acknowledged death certificate was published: either every attempt failed, or it
				 * went out downgraded to QoS 0 and carries no guarantee. A clean DISCONNECT would tell the MQTT
				 * server we left gracefully and suppress the Will, leaving subscribers with nothing better than that
				 * unacknowledged publish - so drop the connection instead and let the server publish the Will it
				 * already holds from connect(). That Will is QoS 1 and retained, so it is strictly the stronger of
				 * the two, and an identical retained payload arriving twice is harmless.
				 */
				if (sendDisconnectPacket && !lwtPublishSucceeded) {
					logger.warn("{}: Could not publish the LWT on {} by any route - closing without a "
							+ "DISCONNECT so the MQTT server publishes the Will instead", getClientId(), lwtTopic);
					sendDisconnectPacket = false;
				}
			}

			/*
			 * Deliberately AFTER the LWT and BEFORE the client is closed. The drain thread has to be alive for a
			 * buffered LWT to reach the MQTT server at all, and dead before the client is closed, or it publishes
			 * into a client that is going away. It also clears the buffer, so anything still queued at this point is
			 * discarded and counted by getPublishBufferDiscardedMessageCount().
			 */
			shutdownPublishBufferDrainThreadQuietly();
			DetachedSession detached = new DetachedSession(client, sendDisconnectPacket, awaitLwtDeliveryToken);
			client = null;
			return detached;
		} finally {
			/*
			 * On every path out, including the one that found no client to detach.
			 *
			 * In a finally so the session is detached even if the LWT path fails unexpectedly - a caller that has
			 * been told the session is gone must not find it still installed.
			 *
			 * And on the no-client path because a connect attempt that is still building its client has
			 * state.inProgress() true with the field still null. This method has just stopped that attempt, so
			 * nothing is in progress any more; leaving the flag set asserted otherwise, and the thread that would
			 * have cleared it was the one being stopped. Every later connect() was then refused at the
			 * autoReconnect gate for the life of the client. connect() re-arms the flag immediately after this
			 * returns, under the same lock, so the extra clear cannot open a window there.
			 */
			state.setInProgress(false);
			lwtDeliveryToken = null;
			// Reset re-subscribed flag
			resubscribed = false;
		}
	}

	/**
	 * Closes a session detached by {@link #detachSession}. <b>Must be called with {@link #clientLock} released.</b>
	 *
	 * Paho's CommsCallback.stop() takes a shortcut only for its own callback thread; every other caller spin waits,
	 * with no cap, until that thread leaves its run loop. Three of the four callbacks dispatched on that thread need
	 * clientLock - connectComplete directly, messageArrived and connectionLost through the application callback - so
	 * holding the lock across this call is a two party deadlock with no timeout on either side.
	 */
	private void closeDetachedSession(DetachedSession detached, long disconnectQuieseTime, long disconnectTimeout)
			throws TahuException {
		if (detached == null) {
			return;
		}

		if (Thread.holdsLock(clientLock)) {
			/*
			 * A tripwire rather than a recovery. There is nothing useful to do here, but the deadlock this guards
			 * against is silent and permanent, so a caller that gets the locking wrong should say so in the log
			 * rather than hang without explanation.
			 */
			logger.error("{}: Closing a detached session while holding clientLock - this can deadlock against Paho's "
					+ "callback thread", getClientId());
		}

		// FIXME - remove This sleep is necessary due to:
		// https://github.com/eclipse/paho.mqtt.java/issues/850
		try {
			Thread.sleep(1000L);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}

		try {
			logger.debug("{}: Disconnecting...", getClientId());
			detached.client.disconnectForcibly(disconnectQuieseTime, disconnectTimeout, detached.sendDisconnectPacket);
			logger.debug("{}: Done disconecting", getClientId());
			detached.client.close();
			logger.debug("{}: Client closed", getClientId());
		} catch (MqttException e) {
			throw new TahuException(TahuErrorCode.INTERNAL_ERROR, e);
		}
	}

	/**
	 * A session that has been detached under clientLock and still needs its Paho client closed outside it.
	 *
	 * awaitLwtDeliveryToken is the death certificate this teardown published, when the caller asked for its
	 * delivery to be confirmed. It travels with the session for the same reason the client does: confirming it
	 * means waiting on Paho's callback thread, which cannot be done holding clientLock.
	 */
	private static final class DetachedSession {

		private final TahuMqttAsyncClient client;
		private final boolean sendDisconnectPacket;
		private final IMqttDeliveryToken awaitLwtDeliveryToken;

		private DetachedSession(TahuMqttAsyncClient client, boolean sendDisconnectPacket,
				IMqttDeliveryToken awaitLwtDeliveryToken) {
			this.client = client;
			this.sendDisconnectPacket = sendDisconnectPacket;
			this.awaitLwtDeliveryToken = awaitLwtDeliveryToken;
		}
	}

	/**
	 * Waits out the configured random startup delay, if there is one.
	 *
	 * Called with no lock held, and deliberately not from inside {@link #attemptConnect}: both of that method's
	 * callers already hold {@link #clientLock} across the call, so a sleep anywhere inside it holds the lock for
	 * the whole delay. RandomStartupDelay is configured in milliseconds and exists to stagger a fleet, so it is
	 * meant to be long - long enough that every publish, teardown and connectComplete on this client would queue
	 * behind a timer, and disconnect() with it.
	 *
	 * The delay is drawn fresh on each call rather than once per client, and this keeps that: clients sharing a
	 * connect retry interval come back from a common outage in lockstep, and only new jitter re-scatters them.
	 *
	 * Taken before the already-connected check rather than after it, which is the one behavioural change. The cost
	 * is a delay paid on an attempt that then finds itself connected; the alternative is to check, release, sleep
	 * and re-acquire, which splits the check from the connect it authorises.
	 */
	private void awaitRandomStartupDelay() {
		if (randomStartupDelay == null || !randomStartupDelay.isValid()) {
			return;
		}

		long randomDelay = randomStartupDelay.getRandomDelay();
		logger.debug("{}: Waiting random delay of {} ms before reconnect attempt", getClientId(), randomDelay);
		try {
			Thread.sleep(randomDelay);
		} catch (InterruptedException e) {
			/*
			 * Restored and swallowed, as it was when this sat inside attemptConnect. The caller goes on to attempt
			 * the connect; changing that to give up is a separate question from where the sleep happens.
			 */
			Thread.currentThread().interrupt();
			logger.warn("{}: Sleep interrupted", getClientId(), e);
		}
	}

	/*
	 * Attempt to connect.
	 */
	private IMqttToken attemptConnect(TahuMqttAsyncClient client, MqttConnectOptions options, String ctx)
			throws MqttSecurityException, MqttException {
		synchronized (clientLock) {
			if (isConnected()) {
				logger.trace("{} is already connected - not trying again", getClientId());
				return null;
			}

			logger.debug("{}: Attempting {} to {}", getClientId(), ctx, getMqttServerUrl());
			logger.trace("{}: Thread {} :: {}", getClientId(), Thread.currentThread().getName(),
					Thread.currentThread().getId());

			// Make the call to connect (this is asynchronous)
			return client.connect(options, ctx, new IMqttActionListener() {

				@Override
				public void onSuccess(IMqttToken token) {
					logger.info("{}: {} succeeded", getClientId(), token.getUserContext());
					state.setInProgress(false);
				}

				@Override
				public void onFailure(IMqttToken token, Throwable throwable) {
					logger.warn("{}: {} failed due to {}", getClientId(), token.getUserContext(),
							throwable != null ? throwable.getMessage() : "?", throwable);
					logger.warn("{}: MQTT Client details: {}", getClientId(), getTahuClientDetails());
					state.setInProgress(false);
				}

				private String getTahuClientDetails() {
					StringBuilder sb = new StringBuilder();
					sb.append("MQTT Server Name = ").append(mqttServerName).append(" :: ");
					sb.append("MQTT Server URL = ").append(mqttServerUrl).append(" :: ");
					sb.append("MQTT Client ID = ").append(clientId).append(" :: ");
					sb.append("Using Birth = ").append(birthTopic == null || birthTopic.isEmpty() ? "false" : "true")
							.append(" :: ");
					sb.append("Using LWT = ").append(lwtTopic == null || lwtTopic.isEmpty() ? "false" : "true");
					return sb.toString();
				}
			});
		}
	}

	/**
	 * A class for tracking the connect in-progress state.
	 */
	private class ConnectingState {

		private boolean inProgress = false;

		protected void setInProgress(boolean inProgress) {
			this.inProgress = inProgress;
		}

		protected boolean inProgress() {
			return this.inProgress;
		}
	}

	/**
	 * A Runnable implementation for connecting the client to a broker. Will continue to attempt to connect on failure
	 * until the client is disconnected (setting the keepConnected flag to false).
	 */
	protected class ConnectRunnable implements Runnable {

		private MqttCallback callback;

		private boolean attemptConnects = true;

		public ConnectRunnable(final MqttCallback callback) {
			this.callback = callback;
		}

		public void stopConnectAttempts() {
			attemptConnects = false;
		}

		@Override
		public void run() {
			// ensure we are disconnected and null
			discardClientForReconnect();

			try {
				// Reset re-subscribed flag
				resubscribed = false;

				if (connectOptions == null) {
					connectOptions = new MqttConnectOptions();
				}
				connectOptions.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
				connectOptions.setCleanSession(cleanSession);
				connectOptions.setConnectionTimeout(30);
				if (getUsername() != null && !getUsername().trim().isEmpty()) {
					logger.debug("{}: Setting username to {}", getClientId(), getUsername());
					connectOptions.setUserName(getUsername());
				}
				if (getPassword() != null && !getPassword().trim().isEmpty()) {
					logger.debug("{}: Setting password to ****", getClientId());
					connectOptions.setPassword(getPassword().toCharArray());
				}
				connectOptions.setKeepAliveInterval(keepAlive);
				if (lwtTopic != null) {
					if (useSparkplugStatePayload) {
						ObjectMapper mapper = new ObjectMapper();
						lastStateDeathPayloadTimestamp = new Date().getTime();
						StatePayload statePayload = new StatePayload(false, lastStateDeathPayloadTimestamp);
						logger.debug("{}: Setting Sparkplug WILL on {} with retain={} and payload={}", getClientId(),
								lwtTopic, lwtRetain, statePayload);
						byte[] payload = mapper.writeValueAsString(statePayload).getBytes();
						connectOptions.setWill(lwtTopic, payload, MqttOperatorDefs.QOS1, lwtRetain);
					} else {
						logger.debug("{}: Setting WILL on {} with retain={}", getClientId(), lwtTopic, lwtRetain);
						connectOptions.setWill(lwtTopic, lwtPayload, MqttOperatorDefs.QOS1, lwtRetain);
					}
				}
				logger.trace("Setting max in-flight messages to {}", getMaxInflightMessages());
				connectOptions.setMaxInflight(getMaxInflightMessages());
				synchronized (messageLock) {
					semaphore = new Semaphore(getMaxInflightMessages(), true);
					lockedMessageSet = ConcurrentHashMap.newKeySet();
				}
				startPublishBufferDrainThread();

				// Create the client instance
				logger.info("{}: Creating the MQTT Client to {} on thread {}", getClientId(), getMqttServerUrl(),
						Thread.currentThread().getName());

				/*
				 * Under clientLock, because this was the one write to the field that was not - and a teardown that
				 * checks the client's identity and then acts on it cannot be made atomic against a writer that
				 * ignores the lock, however few acquisitions the teardown uses. The callback is set inside the same
				 * block so no reader can see a client without one. The connect itself stays outside: it is network
				 * work and must not be done holding this lock.
				 */
				synchronized (clientLock) {
					client = new TahuMqttAsyncClient(getMqttServerUrl().toString(), getClientId().toString(), null);
					client.setCallback(callback);
				}
				IMqttToken connectToken = null;

				// A time stamp to track the current attempt in case the underlying client is stuck attempting forever
				long attemptTimestamp = System.currentTimeMillis();

				if (autoReconnect) {
					try {
						while (!isConnected() && attemptConnects) {
							try {
								// Inside the loop, outside the lock: once per attempt, as before, but without
								// holding clientLock for the length of it
								awaitRandomStartupDelay();

								synchronized (clientLock) {
									if (!attemptConnects) {
										logger.info("{}: No longer attempting to connect", getClientId());
										state.setInProgress(false);
										return;
									}

									connectToken = attemptConnect(client, connectOptions, "connect with retry");

									// Update time stamp for current attempt
									attemptTimestamp = System.currentTimeMillis();
								}

								// Sleep for the connect retry interval
								Thread.sleep(getConnectRetryInterval());
							} catch (InterruptedException ie) {
								Thread.currentThread().interrupt();
								logger.info("{}: Connect thread {} interrupted - giving up",
										Thread.currentThread().getName(), getClientId());
								return;
							} catch (MqttException e) {
								if (e.getReasonCode() == MqttException.REASON_CODE_CONNECT_IN_PROGRESS) {
									if (connectToken != null) {
										logger.debug("{}: Still trying to connect - isComplete? {}, sessionPresent? {}",
												getClientId(), connectToken.isComplete(),
												connectToken.getSessionPresent());
									} else {
										logger.debug("{}: Still trying to connect", getClientId());
									}

									// Check if the connect attempt has timed out
									if (System.currentTimeMillis() - attemptTimestamp > connectAttemptTimeout) {
										synchronized (clientLock) {
											// Forcibly close the client
											logger.warn("{}: Connect attempt has timed out - forcing close",
													getClientId());
											client.close(true);
										}
									} else {
										Thread.sleep(500);
									}
								} else {
									logger.debug("{}: Unable to connect due to {}, next connect attempt in {} ms",
											getClientId(), e.getMessage(), getConnectRetryInterval());
									Thread.sleep(getConnectRetryInterval());
								}
							}
						}

						logger.info("{}: MQTT Client connected to {} on thread {}", getClientId(), getMqttServerUrl(),
								Thread.currentThread().getName());
						state.setInProgress(false);
					} catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
						logger.info("{}: Connect thread 2 interrupted - giving up", getClientId());
						state.setInProgress(false);
						return;
					} catch (Throwable throwable) {
						logException(
								"Error while attempting connect (with autoReconnect=true) to " + getMqttServerUrl(),
								throwable);
						state.setInProgress(false);
						if (autoReconnect && !isConnected() && attemptConnects) {
							attemptRecovery();
						}
					}
				} else {
					try {
						awaitRandomStartupDelay();

						synchronized (clientLock) {
							if (!attemptConnects) {
								logger.info("{}: No longer attempting to connect", getClientId());
								state.setInProgress(false);
								return;
							}

							// Attempt to connect
							attemptConnect(client, connectOptions, "connect");
						}
					} catch (Throwable throwable) {
						logException(
								"Error while attempting connect (with autoReconnect=false) to " + getMqttServerUrl(),
								throwable);
					}
				}
			} catch (Exception e) {
				logger.error("{}: Error while connecting client", getClientId(), e);
				state.setInProgress(false);
				if (autoReconnect && !isConnected() && attemptConnects) {
					attemptRecovery();
				}
			}
		}
	}

	private void attemptRecovery() {
		logger.warn("{}: Connect failed - retrying", getClientId());
		try {
			if (randomStartupDelay != null && randomStartupDelay.isValid()) {
				long randomDelay = randomStartupDelay.getRandomDelay();
				logger.info("{}: Sleeping {} before reconnect attempt", getClientId(), randomDelay);
				Thread.sleep(randomDelay);
			} else {
				Thread.sleep(getConnectRetryInterval());
			}
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
			logger.warn("{}: InterruptedException while preparing to reconnect", getClientId(), ie);
			return;
		}
		if (autoReconnect) {
			connect();
		} else {
			logger.warn("{}: AutoReconnect canceled - No longer going to retry", getClientId());
			return;
		}
	}

	private class AsyncPublisher implements Runnable {

		private String topic;
		private byte[] payload;
		private int qos;
		private boolean retained;

		// Retry params
		private boolean retry = false;
		private long retryDelay;
		private int numAttempts;

		public AsyncPublisher(String topic, byte[] payload, int qos, boolean retained, boolean retry, long retryDelay,
				int numAttempts) {
			this.topic = topic;
			this.payload = payload;
			this.qos = qos;
			this.retained = retained;
			this.retry = retry;
			this.retryDelay = retryDelay;
			this.numAttempts = numAttempts;
		}

		@Override
		public void run() {
			try {
				if (retry) {
					/*
					 * Stops at the first attempt the client accepts, which is what makes this a retry rather than a
					 * repeat. publishOrBuffer() accepts a message either by sending it or by queueing it, and a
					 * queued message is already on its way - resubmitting then appends a second copy of the same
					 * message to the buffer, and every further attempt appends another. Only an outright rejection,
					 * which today means the buffer is at capacity, is worth another attempt.
					 */
					boolean accepted = false;
					for (int i = 0; i < numAttempts && !accepted; i++) {
						if (client == null || !client.isConnected()) {
							Thread.sleep(retryDelay);
						} else {
							accepted = handlePublish();
							if (!accepted && i < numAttempts - 1) {
								// The retry budget has to span time to be worth anything - a rejection is a full
								// buffer, and back-to-back attempts give it no chance to drain. Not after the last
								// attempt, which has nothing left to wait for.
								Thread.sleep(retryDelay);
							}
						}
					}

					if (!accepted) {
						logger.error("{}: Failed to publish message on {} after {} attempts", getClientId(), topic,
								numAttempts);
						throw new TahuException(TahuErrorCode.INTERNAL_ERROR,
								"Failed to publish message on " + topic + " after " + numAttempts + " attempts");
					}
				} else {
					if (client == null) {
						throw new TahuException(TahuErrorCode.INTERNAL_ERROR, "MQTT client is null");
					} else if (client.isConnected()) {
						handlePublish();
					} else {
						throw new TahuException(TahuErrorCode.INTERNAL_ERROR, "MQTT client not connected");
					}
				}
			} catch (Exception e) {
				logger.error("{}: Failed to publish", getClientId(), e);
			}
		}

		/**
		 * @return true if the client took responsibility for the message - either sent inline or queued in the
		 *         publish buffer, which {@link #publish(String, byte[], int, boolean)} signals with a null token -
		 *         queued rather than lost. False only if it was rejected outright and nothing holds it, which is the
		 *         one outcome worth another attempt.
		 */
		private boolean handlePublish() throws Exception {
			try {
				// Same path as publish() so async publishes cannot jump ahead of anything already buffered
				publishOrBuffer(topic, payload, qos, retained);
				return true;
			} catch (TahuException e) {
				// Logged rather than rethrown: the caller is on another thread and has nothing to catch it
				logger.error("{}: Failed to publish on {} - {}", getClientId(), topic, e.getMessage());
				return false;
			}
		}
	}

	private void shutdownConnectionMonitorThread() {
		if (connectionMonitorThread == null) {
			logger.debug("{}: Not shutting down ConnectionMonitorThread - its null", getClientId());
			return;
		}
		if (connectionMonitorThread.isAlive()) {
			logger.debug("{}: Shutting down ConnectionMonitorThread", getClientId());
			connectionMonitorThread.shutdown();
			connectionMonitorThread = null;
		} else {
			logger.debug("{}: Not shutting down ConnectionMonitorThread - its not alive", getClientId());
		}
	}

	private class ConnectionMonitorThread extends Thread {
		private ConnectionMonitor connectionMonitor;

		public ConnectionMonitorThread(ConnectionMonitor connectionMonitor) {
			super(connectionMonitor);
			this.connectionMonitor = connectionMonitor;
		}

		public void shutdown() {
			connectionMonitor.setKeepRunning(false);
			this.interrupt();
		}
	}

	private class ConnectionMonitor implements Runnable {

		private final TahuMqttAsyncClient monitoredClient;
		private final MqttClientId monitoredClientId;
		private boolean keepRunning = true;

		public ConnectionMonitor(TahuMqttAsyncClient client, MqttClientId clientId) {
			this.monitoredClient = client;
			this.monitoredClientId = clientId;
		}

		public void setKeepRunning(boolean keepRunning) {
			this.keepRunning = keepRunning;
		}

		public void run() {
			try {
				int connectionLostCounter = 0;
				while (keepRunning) {
					synchronized (clientLock) {
						if (monitoredClient != null) {
							if (!monitoredClient.isConnected()) {
								if (state.inProgress()) {
									logger.debug("{}: ConnectionMonitor - Attempting to connect", monitoredClientId);
									connectionLostCounter = 0;
								} else {
									logger.debug("{}: ConnectionMonitor - Not connected, incrementing counter",
											monitoredClientId);
									connectionLostCounter++;
								}
							} else {
								logger.trace("{}: ConnectionMonitor - Already connected", monitoredClientId);
								connectionLostCounter = 0;
							}
						} else {
							logger.debug("{}: ConnectionMonitor - Client is null - Uncaught connectionLost",
									getClientId());
							connectionLostCounter = 5;
						}
					}

					if (connectionLostCounter == 5 && callback != null) {
						callback.connectionLost(mqttServerName, mqttServerUrl, monitoredClientId,
								new Throwable(monitoredClientId + ": Uncaught paho disconnect"));
					}

					try {
						Thread.sleep(DEFAULT_CONNECT_MONITOR_INTERVAL);
					} catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
						logger.debug("{}: ConnectionMonitor interrupted", monitoredClientId);
					}
				}
			} catch (Exception e) {
				logger.error("{}: ConnectionMonitor failed to keep running", monitoredClientId, e);
			}
		}
	}

	@Override
	public void connectComplete(boolean reconnect, String serverURI) {

		// Check if we are in the process of disconnecting
		if (isDisconnectInProgress()) {
			logger.warn("{}: Ignoring connect complete to {}, disconnect in progress", getClientId(), serverURI);
			// This potentially prevents a deadlock situation upon synchronizing on the clientLock below if a disconnect
			// is in progress and waiting on the client.disconnect() call
			return;
		}

		synchronized (clientLock) {
			if (reconnect) {
				logger.debug("{}: SUCCESSFULLY RECONNECTED to {}", getClientId(), getMqttServerUrl());
			}

			if (autoReconnect) {
				if (connectionMonitorThread == null || !connectionMonitorThread.isAlive()) {
					connectionMonitorThread = new ConnectionMonitorThread(new ConnectionMonitor(client, getClientId()));
					connectionMonitorThread.start();
				}
			}

			// The client is connected - renew online date, renew the connect time, clear disconnect time
			this.renewOnlineDate();
			this.renewConnectTime();
			this.clearDisconnectTime();

			logger.info("{}: Connected to {}", getClientId(), getMqttServerUrl());

			// Call connectComplete() with the callback
			getCallback().connectComplete(reconnect, getMqttServerName(), getMqttServerUrl(), getClientId());

			// Subscribe (or re-subscribe)
			if (!subscriptions.isEmpty()) {
				// Build up the arrays of topics and QoS levels
				int totalCount = subscriptions.size();
				int subscribedCount = 0;
				ArrayList<String> topicsList = new ArrayList<String>(subscriptions.keySet());

				while (subscribedCount < totalCount) {
					int topicsRemaining = totalCount - subscribedCount;
					// Don't attempt to publish more that the max topics per subscribe
					int size = topicsRemaining > maxTopicsPerSubscribe ? maxTopicsPerSubscribe : topicsRemaining;

					String[] topics = new String[size];
					int[] qosLevels = new int[size];

					for (int i = 0; i < size; i++) {
						String topic = topicsList.get(i + subscribedCount);
						topics[i] = topic;
						qosLevels[i] = subscriptions.get(topic);
					}

					String topicStr = Arrays.toString(topics);
					String qosStr = Arrays.toString(qosLevels);
					logger.debug("{}: server {} - Attempting to subscribe on topic {} with QoS={}", getClientId(),
							getMqttServerName(), topicStr, qosStr);
					try {
						client.subscribe(topics, qosLevels, null, new IMqttActionListener() {
							@Override
							public void onSuccess(IMqttToken asyncActionToken) {
								int[] grantedQos = asyncActionToken.getGrantedQos();
								if (Arrays.equals(qosLevels, grantedQos)) {
									logger.debug("{}: server {} - Successfully subscribed on {} on QoS={}",
											getClientId(), getMqttServerName(), topicStr, qosStr);
								} else {
									try {
										String grantedQosStr = Arrays.toString(grantedQos);
										logger.error("{}: server {} - Failed subscribe on {} granted QoS {} != {}",
												getClientId(), getMqttServerName(), topicStr, qosStr, grantedQosStr);

										// FIXME - remove This sleep is necessary due to:
										// https://github.com/eclipse/paho.mqtt.java/issues/850
										Thread.sleep(1000);

										/*
										 * Under clientLock, unlike the teardowns in discardClientForReconnect()
										 * and closeDetachedSession(). Not an exception to that rule: Paho
										 * dispatches action listeners on its callback thread, so
										 * CommsCallback.stop() takes its same thread shortcut and waits for
										 * nothing. Moving this off that thread would need the same split.
										 */
										synchronized (clientLock) {
											if (client != null) {
												// Force the disconnect and return
												client.disconnectForcibly(0, 1, false);
											}
										}
										return;
									} catch (Exception e) {
										logger.error(
												"{}: server {} - Failed disconnect on failed subscribe granted QoS",
												getClientId(), getMqttServerName(), e);
									}
								}
							}

							@Override
							public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
								synchronized (clientLock) {
									try {
										if (client != null) {
											logger.error(
													"{}: server {} - Failed to subscribe on {} - forcing disconnect",
													getClientId(), getMqttServerName(), topicStr);
											client.disconnectForcibly(0, 1, false);
										} else {
											logger.error("{}: server {} - Failed to subscribe on {} - client is null",
													getClientId(), getMqttServerName(), topicStr);
										}
									} catch (MqttException e) {
										logger.error("{}: server {} - Failed disconnect on failed subscribe",
												getClientId(), getMqttServerName(), e);
									}
								}
							}

						});
					} catch (MqttException e) {
						logger.error("{}: server {} - Failed to subscribe on {} with QoS={}", getClientId(),
								getMqttServerName(), topicStr, qosStr, e);
						break;
					}

					subscribedCount += size;

				}
			} else {
				if (trackFirstConnection && !firstConnection) {
					logger.warn("{}: No subscriptions for {}", getClientId(), getClientId());
				}
			}

			// Mark that the client has finished re-subscribing
			resubscribed = true;

			// Publish a standard Birth/Death Certificate if a baseTopic has been defined.
			if (onlineState) {
				publishBirthMessage();
			} else {
				try {
					/*
					 * Published without waiting for the acknowledgement, unlike every other caller that asks for a
					 * confirmed death certificate. This runs on Paho's callback thread, and deliveryComplete() -
					 * the callback that would clear the token and end the wait - is dispatched by that same thread.
					 * So the wait cannot be satisfied from here: it runs its full keepAlive * 4 quarter seconds -
					 * keepAlive seconds, 30 by default - and returns having confirmed nothing, with clientLock held
					 * throughout.
					 *
					 * The rule is the one already recorded on birthRecoveryThread, for the same reason and the same
					 * thread - it was written for publishBirthMessage() and applies just as much to this arm of the
					 * same if. Moving the wait off clientLock would not help: the confirmation is unobtainable on
					 * this thread whatever the lock is doing.
					 *
					 * setOnlineState() makes the same call and keeps its wait. Reached from an application thread,
					 * the confirmation can arrive there, so that site costs lock hold time rather than confirming
					 * nothing - a different fix, and left alone here. See the note on it.
					 */
					this.publishLwt(false);
				} catch (Exception e) {
					logger.error("Failed to publish the LWT", e);
				}
			}

			firstConnection = false;
		}
	}

	/**
	 * Sets the 'track first connection' flag
	 * 
	 * @param trackFirstConnection - the 'track first connection' flag as {@link boolean}
	 */
	public void setTrackFirstConnection(boolean trackFirstConnection) {
		synchronized (clientLock) {
			this.trackFirstConnection = trackFirstConnection;
		}
	}

	public void setOnlineState(boolean newOnlineState) {
		synchronized (clientLock) {
			if (this.onlineState == newOnlineState) {
				return;
			} else {
				this.onlineState = newOnlineState;

				if (onlineState) {
					publishBirthMessage();
				} else {
					try {
						/*
						 * Left waiting under clientLock, deliberately, unlike the same call in connectComplete().
						 *
						 * The difference is who can end the wait. This runs on an application thread, so Paho's
						 * callback thread stays free to dispatch the deliveryComplete() that clears the token - the
						 * wait is satisfiable here, where on the callback thread it never was. What it costs is lock
						 * hold time, up to keepAlive seconds and only when nothing acknowledges, rather than the
						 * guaranteed full-budget stall that made the other site worth changing.
						 *
						 * A confirmed death certificate is worth that, so the fix here is not to drop the wait but to
						 * take it outside the lock the way disconnectSession() does: publish under clientLock,
						 * record the token, wait on that token once the lock is released. Pre-existing, and left for
						 * its own change rather than folded into this one.
						 */
						this.publishLwt(true);
					} catch (Exception e) {
						logger.error("Failed to publish the LWT when setting the online state", e);
					}
				}
			}
		}
	}

	/**
	 * Publishes the BIRTH, or forces a reconnect if the acknowledged path cannot take it right now.
	 *
	 * The backpressure check happens BEFORE publishing, for the same reason as
	 * {@link #publishLwtWithFallback(byte[])}: {@link #publishOrBuffer(String, byte[], int, boolean)} appends to the
	 * buffer and then returns null, so reacting to the null afterwards would leave a queued copy behind and could
	 * deliver the BIRTH twice.
	 *
	 * Where the LWT falls back to QoS 0, this falls back to a reconnect. The death certificate has no second chance -
	 * the buffer is torn down moments later and a clean DISCONNECT suppresses the Will - but a BIRTH does: the
	 * session it announces is the thing being restarted, so dropping the connection and letting connect() publish it
	 * again on a fresh session is both simpler and more correct than announcing a session whose BIRTH never went out.
	 *
	 * A buffered BIRTH is not a published BIRTH. Before the publish buffer existed this could not arise, because
	 * publish() blocked until the message was handed to Paho, so "returned without throwing" meant "on the wire" -
	 * which is the behaviour this method was written against, and why it recovers only from an exception.
	 */
	public void publishBirthMessage() {
		/*
		 * Checked before clientLock, not inside it. A session being torn down has no use for a BIRTH, and the
		 * teardown holds this lock across a bounded buffer drain - so a callback thread arriving here would park on
		 * it for that long, stalling every other callback for this client, only to find a session that is gone.
		 */
		if (isDisconnectInProgress()) {
			logger.debug("{}: Not publishing the BIRTH - a disconnect is in progress", getClientId());
			return;
		}

		synchronized (clientLock) {
			/*
			 * client is null checked as well as birthTopic. The escalation below tears the session down through
			 * disconnect(), which clears the client field, so a second call - setOnlineState() after a teardown, say -
			 * would otherwise dereference null here. publishLwt() guards the same way.
			 */
			if (birthTopic == null || client == null || !client.isConnected()) {
				return;
			}

			/*
			 * The two conditions publishOrBuffer() buffers on. Reached from setOnlineState() mid-session, where the
			 * window can be exhausted; connect() rebuilds the semaphore before the client exists, so the
			 * connectComplete() route always finds a full window and an empty buffer.
			 *
			 * Both are the ordinary backpressure the buffer exists to absorb and both clear on the next
			 * acknowledgement, so a moment's wait is worth taking rather than letting a message queued for
			 * milliseconds cost the whole session. That wait is taken on a worker: see birthRecoveryThread.
			 */
			if (getAvailablePublishPermits() == 0 || getPublishBufferDepth() > 0) {
				startBirthRecovery(true);
				return;
			}

			try {
				publishBirth();
			} catch (TahuException ce) {
				logger.error("{}: Error in birth topic publish on connect", getClientId(), ce);
				startBirthRecovery(false);
			}
		}
	}

	/**
	 * Publishes the BIRTH inline. Callers must hold {@link #clientLock} and must have established that there is a
	 * client, a birth topic and a publishable window.
	 *
	 * @throws TahuException if the BIRTH could not be handed to the MQTT client
	 */
	private void publishBirth() throws TahuException {
		synchronized (clientLock) {
			if (useSparkplugStatePayload) {
				byte[] payload;
				try {
					ObjectMapper mapper = new ObjectMapper();
					StatePayload statePayload = new StatePayload(true, lastStateDeathPayloadTimestamp);
					logger.debug("{}: Publishing Sparkplug BIRTH on {} with retain={} and payload: {}", getClientId(),
							birthTopic, birthRetain, statePayload);
					payload = mapper.writeValueAsString(statePayload).getBytes();
				} catch (Exception e) {
					// Reconnecting cannot fix a payload that will not encode, so there is nothing to recover
					logger.error("{}: Failed to encode the BIRTH message on {}", getClientId(), birthTopic, e);
					return;
				}

				/*
				 * Deliberately outside the encode handler above - a failed publish must reach the caller's recovery
				 * rather than being logged and swallowed.
				 */
				publish(birthTopic, payload, MqttOperatorDefs.QOS1, birthRetain);
			} else {
				logger.debug("{}: Publishing BIRTH on {} with retain={}", getClientId(), birthTopic, birthRetain);
				publish(birthTopic, birthPayload, MqttOperatorDefs.QOS1, birthRetain);
			}
		}
	}

	/**
	 * Starts the off-thread BIRTH recovery: optionally waits for a publishable window and retries, and tears the
	 * session down if the BIRTH still cannot go out.
	 *
	 * Always on a worker, never on the calling thread. Two of the three routes into {@link #publishBirthMessage()} -
	 * connectComplete() and the STATE correction in the host callback - arrive on Paho's single callback thread, and
	 * that is the thread deliveryComplete() runs on. Waiting there blocks the only thread that could return the
	 * permit being waited for, and tearing down there re-enters connect() from inside connectComplete().
	 *
	 * @param awaitWindow true to wait for the backpressure to clear and retry, false to escalate immediately
	 */
	private void startBirthRecovery(boolean awaitWindow) {
		synchronized (clientLock) {
			/*
			 * One recovery per session, not one per live thread. A worker started for an earlier session can still
			 * be alive here; suppressing on that would drop a new session's genuine BIRTH failure.
			 */
			if (birthRecoveryThread != null && birthRecoveryThread.isAlive() && birthRecoverySession == client) {
				logger.debug("{}: BIRTH recovery already running for this session", getClientId());
				return;
			}

			birthRecoverySession = client;
			birthRecoveryThread = new Thread(new BirthRecovery(awaitWindow, client),
					"TahuBirthRecovery-" + getClientId().getMqttClientId());
			birthRecoveryThread.setDaemon(true);
			birthRecoveryThread.start();
		}
	}

	private class BirthRecovery implements Runnable {

		private final boolean awaitWindow;

		/*
		 * The session this worker was started for. Everything below acts on it by identity rather than on whatever
		 * happens to be installed when the worker wakes: the wait is bounded but the lock that follows it is not, and
		 * a session can be lost and replaced in that gap. Acting on the wrong one would publish a second retained
		 * BIRTH onto a session that already announced itself, or tear down a healthy one.
		 */
		private final TahuMqttAsyncClient session;

		BirthRecovery(boolean awaitWindow, TahuMqttAsyncClient session) {
			this.awaitWindow = awaitWindow;
			this.session = session;
		}

		@Override
		public void run() {
			// Outside clientLock: the permit this waits for is returned by deliveryComplete(), which needs it too.
			if (awaitWindow) {
				awaitPublishableWindow(BIRTH_BACKPRESSURE_WAIT);
			}

			synchronized (clientLock) {
				if (client != session) {
					// The failure this worker was started for belongs to a session that is no longer live.
					logger.debug("{}: Abandoning BIRTH recovery - the session it was started for is gone",
							getClientId());
					return;
				}

				if (birthTopic == null || !client.isConnected()) {
					return;
				}

				if (awaitWindow && getAvailablePublishPermits() > 0 && getPublishBufferDepth() == 0) {
					try {
						publishBirth();
						return;
					} catch (TahuException ce) {
						logger.error("{}: BIRTH publish failed after the window opened", getClientId(), ce);
					}
				}

				logger.error("{}: Could not publish the BIRTH on {} - tearing the session down", getClientId(),
						birthTopic);
			}

			/*
			 * Outside clientLock, and scoped to this session. disconnectSession() re-checks the identity under the
			 * lock, so losing the race here costs nothing.
			 *
			 * sendDisconnect false so the MQTT server publishes the Will rather than treating this as a graceful
			 * exit, and publishLwt false because there is no acknowledged death to send from a session that never
			 * announced itself - the Will is the better carrier.
			 */
			boolean tornDown;
			try {
				tornDown = disconnectSession(session, 0, 1, false, false, false);
			} catch (Exception e) {
				/*
				 * The close failed, but detachSession() had already cleared the session before anything that can
				 * throw ran - so the client is gone and recovery still has to be re-armed. Treating a throw as "not
				 * torn down" is what left a host permanently offline whenever close() raced the socket teardown.
				 */
				logger.error("{}: Failed to close the session after a failed BIRTH publish", getClientId(), e);
				tornDown = true;
			}

			/*
			 * Re-arm recovery. disconnect() stops the connection monitor, stops the connect runnable and clears the
			 * client field, and a locally requested disconnect produces no connectionLost from Paho -
			 * ClientComms.shutdownConnection passes a null cause, and CommsCallback only forwards a non-null one. A
			 * host application whose only reconnect is the connectionLost callback would otherwise stay offline for
			 * good, with a retained offline STATE on its topic. Clients that poll isConnected() in their own run
			 * loop are unaffected either way.
			 */
			if (tornDown) {
				getCallback().connectionLost(getMqttServerName(), getMqttServerUrl(), getClientId(),
						new Throwable("BIRTH publish failed"));
			}
		}
	}

	/**
	 * Publishes the LWT at its configured QoS, falling back to QoS 0 if the acknowledged path cannot take it.
	 *
	 * The LWT is the last thing this client sends, the publish buffer is torn down moments later, and on a clean
	 * DISCONNECT the MQTT server suppresses the Will - so the explicit publish is the only death certificate there
	 * will be. A buffered or rejected LWT is therefore as good as lost, where for ordinary data either outcome is
	 * recoverable.
	 *
	 * The backpressure check happens BEFORE publishing, not after. {@link #publishOrBuffer(String, byte[], int,
	 * boolean)} appends to the buffer and then returns null, so reacting to the null afterwards would leave a queued
	 * copy behind and could deliver the LWT twice - once at QoS 0 here and once from the drain thread. Testing the
	 * two conditions that cause buffering up front avoids that.
	 *
	 * A genuine failure - not connected, no client - is still rethrown so it reaches disconnect(), which is what 3.x
	 * intended by moving the publish outside the encode handler.
	 */
	private IMqttDeliveryToken publishLwtWithFallback(byte[] payload) throws TahuException {
		// The two conditions publishOrBuffer() buffers on: an exhausted in-flight window, or a non-empty buffer
		// that this message would have to queue behind to keep ordering.
		if (lwtQoS > MqttOperatorDefs.QOS0 && (getAvailablePublishPermits() == 0 || getPublishBufferDepth() > 0)) {
			return publishLwtAtQos0(payload);
		}

		IMqttDeliveryToken token;
		try {
			token = publish(lwtTopic, payload, lwtQoS, lwtRetain);
		} catch (TahuException e) {
			token = publishLwtAtQos0(payload);
			if (token == null) {
				throw e;
			}
			return token;
		}

		// Belt and braces for the race the check above cannot close: a permit can be taken, or the buffer filled,
		// between the check and the publish. Rare, and a duplicate retained death certificate is harmless.
		return token != null ? token : publishLwtAtQos0(payload);
	}

	/*
	 * Republishes the LWT at QoS 0. QoS 0 needs no in-flight permit and is never buffered, so it can still leave the
	 * process when the acknowledged path cannot. The acknowledgement is given up in exchange, which is the right
	 * trade for a retained message - a subscriber that connects later still reads it from the MQTT server.
	 */
	private IMqttDeliveryToken publishLwtAtQos0(byte[] payload) {
		lwtDowngradedToQos0 = true;
		logger.warn("{}: LWT on {} cannot be published at QoS {} - the in-flight window is exhausted or the publish "
				+ "buffer is in use. Retrying at QoS 0 so the death certificate is not lost.", getClientId(), lwtTopic,
				lwtQoS);
		try {
			return publish(lwtTopic, payload, MqttOperatorDefs.QOS0, lwtRetain);
		} catch (Exception e) {
			logger.error("{}: Failed to publish the LWT on {} at QoS 0", getClientId(), lwtTopic, e);
			return null;
		}
	}

	public void publishLwt(boolean waitForLwt) throws MqttException, TahuException {
		/*
		 * Same reasoning as publishBirthMessage(), with one difference that matters: the disconnect path publishes
		 * the death certificate through publishLwtNow() rather than through here. Guarding that call too would skip
		 * the LWT on every disconnect, which is the one moment it exists for.
		 */
		if (isDisconnectInProgress()) {
			logger.debug("{}: Not publishing the LWT - a disconnect is in progress and will publish it", getClientId());
			return;
		}

		publishLwtNow(waitForLwt);
	}

	/**
	 * Publishes the LWT unconditionally, for the disconnect path that is itself the reason a disconnect is in
	 * progress. Every other caller goes through {@link #publishLwt(boolean)}.
	 *
	 * @return the death certificate this call published, or null if it published nothing - the topic was unset, the
	 *         client was already down, the payload would not encode, or the publish failed. A caller that means to
	 *         confirm the certificate must wait on this token rather than on {@link #lwtDeliveryToken}: the field is
	 *         the live token and is cleared by every teardown, so a wait keyed off it can be satisfied by a publish
	 *         that is not the one being confirmed - or, as it was, by its own teardown clearing it.
	 */
	private IMqttDeliveryToken publishLwtNow(boolean waitForLwt) throws MqttException, TahuException {
		IMqttDeliveryToken published = null;
		synchronized (clientLock) {
			boolean clientConnected = client != null && client.isConnected();
			boolean lwtDeliveryComplete = false;
			lwtDowngradedToQos0 = false;
			// Nothing attempted yet means nothing for disconnect() to escalate over
			lwtPublishSucceeded = true;
			if (lwtTopic != null && clientConnected) {
				boolean lwtPublished = false;
				// Flipped back to true below only if the message actually reaches the MQTT client
				lwtPublishSucceeded = false;
				synchronized (lwtDeliveryLock) {
					/*
					 * Synchronization with the deliveryComplete() callback is needed to ensure that
					 * the publish() call is fully completed and the lwtDeliveryToken is set before
					 * it is being nullified in the Paho callback.
					*/
					if (useSparkplugStatePayload) {
						byte[] payload;
						try {
							ObjectMapper mapper = new ObjectMapper();
							StatePayload statePayload = new StatePayload(false, lastStateDeathPayloadTimestamp);
							logger.debug("{}: Publishing Sparkplug LWT on {} with qos={} and retain={} and payload: {}",
									getClientId(), lwtTopic, lwtQoS, lwtRetain, statePayload);
							payload = mapper.writeValueAsString(statePayload).getBytes();
						} catch (Exception e) {
							// Reconnecting cannot fix a payload that will not encode, so there is nothing to recover
							logger.error("{}: Failed to encode the LWT message on {}", getClientId(), lwtTopic, e);
							return null;
						}

						/*
						 * Deliberately outside the encode handler above so a failed publish reaches the caller rather
						 * than being logged and swallowed.
						 */
						published = lwtDeliveryToken = publishLwtWithFallback(payload);
					} else {
						logger.debug("{}: Publishing LWT on {} with qos={} and retain={}", getClientId(), lwtTopic,
								lwtQoS, lwtRetain);
						published = lwtDeliveryToken = publishLwtWithFallback(lwtPayload);
					}
					if (lwtDeliveryToken != null) {
						logger.debug("{}: published on LWT Topic={}, messageId={}", getClientId(), lwtTopic,
								lwtDeliveryToken.getMessageId());
						lwtPublished = true;

						/*
						 * A QoS 0 token means Paho accepted the message, not that the MQTT server received it - there
						 * is no PUBACK, and this path is only reached when that server is already not acknowledging.
						 * Treating it as success sent a clean DISCONNECT, which told the server to suppress the Will
						 * it has held since connect() - so the one remaining chance was spent to protect a publish
						 * that carries no guarantee. The Will is registered at QoS 1, retained, from the same
						 * payload and timestamp, so it is the better of the two; and a duplicate retained death
						 * certificate is harmless, so keeping both costs nothing.
						 */
						lwtPublishSucceeded = !lwtDowngradedToQos0;
					} else {
						logger.warn("Failed to publish LWT {}", lwtTopic);
					}
				}

				if (lwtPublished && waitForLwt) {
					lwtDeliveryComplete = isLwtDeliveryComplete();
					logger.trace("{}: Completed LWT Delivery? {}", getClientId(), lwtDeliveryComplete);
				} else {
					logger.trace("{}: Not waiting for LWT", getClientId());
				}
			} else {
				logger.debug("{}: Not publishing LWT, client connected state: {}", getClientId(), clientConnected);
			}
		}

		return published;
	}

	private Date getConnectTime() {
		return this.connectTime;
	}

	private Date getDisconnectTime() {
		return this.disconnectTime;
	}

	private void clearConnectTime() {
		this.connectTime = null;
	}

	private void clearDisconnectTime() {
		this.disconnectTime = null;
	}

	private void renewConnectTime() {
		this.connectTime = new Date();
	}

	private void renewDisconnectTime() {
		this.disconnectTime = new Date();
	}

	private long getConnectRetryInterval() {
		return connectRetryInterval;
	}

	public void setConnectRetryInterval(long connectRetryInterval) {
		this.connectRetryInterval = connectRetryInterval;
	}

	private long getConnectAttemptTimeout() {
		return connectAttemptTimeout;
	}

	public void setConnectAttemptTimeout(long connectAttemptTimeout) {
		this.connectAttemptTimeout = connectAttemptTimeout;
	}

	public boolean isAttemptingConnect() {
		return state.inProgress();
	}

	private String getErrorMessage(String prefix, Throwable throwable) {
		return new StringBuilder(prefix).append(": ").append(getErrorMessage(throwable)).toString();
	}

	private String getErrorMessage(Throwable throwable) {
		StringBuilder sb = new StringBuilder(throwable.getMessage());
		Throwable cause = throwable.getCause();
		if (cause != null) {
			sb.append(": ").append(getErrorMessage(cause));
		}
		return sb.toString();
	}

	private void logException(String message, Throwable throwable) {
		String errorMessage = getErrorMessage(message, throwable);
		if (logger.isTraceEnabled()) {
			// Only log the stack trace if trace is enabled
			logger.error("{}: {}", getClientId(), errorMessage, throwable);
		} else {
			logger.error("{}: {}", getClientId(), errorMessage);
		}
	}

	/**
	 * Waits for one specific death certificate to be acknowledged, with no lock held.
	 *
	 * Bound to the token it was given, and asks that token whether it is complete rather than watching
	 * {@link #lwtDeliveryToken}. The field cannot answer this question: it is the live LWT token, every teardown
	 * clears it on its way out, and another teardown can replace it - two can hold claims at once. Keyed off the
	 * field this wait returned immediately, because the teardown that published the certificate cleared the field
	 * before releasing the lock this wait was waiting to be free of.
	 *
	 * Paho completes the token when the PUBACK arrives, from its comms thread rather than from the callback thread
	 * that dispatches {@link #deliveryComplete(IMqttDeliveryToken)}, so the confirmation no longer depends on a
	 * thread this class has just been restructured to stop blocking. A QoS 0 token completes on write and confirms
	 * nothing, which is why a downgraded certificate is refused a clean DISCONNECT by lwtPublishSucceeded rather
	 * than by anything measured here.
	 *
	 * Same budget as {@link #isLwtDeliveryComplete()}: keepAlive * 4 quarter seconds. The difference is that
	 * nothing is blocked while it runs.
	 */
	private void awaitLwtDelivery(IMqttDeliveryToken expected) {
		int counter = keepAlive * 4;
		for (int i = 0; i < counter; i++) {
			if (expected.isComplete()) {
				logger.info("{}: LWT delivery confirmation - done waiting", getClientId());
				return;
			}
			try {
				Thread.sleep(250);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				logger.warn("{}: Interrupted while waiting for LWT", getClientId());
				return;
			}
		}

		logger.warn("{}: LWT delivery confirmation - timeout", getClientId());
	}

	private boolean isLwtDeliveryComplete() {
		int counter = keepAlive * 4;
		for (int i = 0; i < counter; i++) {
			try {
				if (lwtDeliveryToken == null) {
					logger.info("{}: LWT delivery confirmation - done waiting", getClientId());
					return true;
				} else {
					Thread.sleep(250);
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				logger.warn("{}: Interrupted while waiting for LWT", getClientId());
			}
		}
		lwtDeliveryToken = null;
		logger.warn("{}: LWT delivery confirmation - timeout", getClientId());
		return false;
	}
}
