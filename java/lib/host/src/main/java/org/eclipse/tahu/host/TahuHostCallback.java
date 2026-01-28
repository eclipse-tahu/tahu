/********************************************************************************
 * Copyright (c) 2022 Cirrus Link Solutions and others
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

package org.eclipse.tahu.host;

import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.tahu.host.api.MultiHostApplicationEventHandler;
import org.eclipse.tahu.host.seq.SequenceReorderManager;
import org.eclipse.tahu.message.PayloadDecoder;
import org.eclipse.tahu.message.model.SparkplugBPayload;
import org.eclipse.tahu.message.model.SparkplugMeta;
import org.eclipse.tahu.message.model.StatePayload;
import org.eclipse.tahu.mqtt.ClientCallback;
import org.eclipse.tahu.mqtt.MqttClientId;
import org.eclipse.tahu.mqtt.MqttServerName;
import org.eclipse.tahu.mqtt.MqttServerUrl;
import org.eclipse.tahu.mqtt.TahuClient;
import org.eclipse.tahu.util.TopicUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TahuHostCallback implements ClientCallback {

	private static Logger logger = LoggerFactory.getLogger(TahuHostCallback.class.getName());

	private static final int DEFAULT_NUM_OF_THREADS = 100;

	private final ThreadPoolExecutor[] sparkplugBExecutors;

	private Map<MqttServerName, TahuClient> tahuClients;

	private final boolean enableSequenceReordering;

	private final MultiHostApplicationEventHandler eventHandler;

	private final CommandPublisher commandPublisher;

	private final SequenceReorderManager sequenceReorderManager;

	private final PayloadDecoder<SparkplugBPayload> payloadDecoder;

	private final String hostId;

	private boolean onlineState;

	public TahuHostCallback(MultiHostApplicationEventHandler eventHandler, CommandPublisher commandPublisher,
			SequenceReorderManager sequenceReorderManager, PayloadDecoder<SparkplugBPayload> payloadDecoder,
			String hostId, boolean onlineState) {
		this.eventHandler = eventHandler;
		this.commandPublisher = commandPublisher;
		if (sequenceReorderManager != null) {
			this.enableSequenceReordering = true;
			this.sequenceReorderManager = sequenceReorderManager;
			this.sequenceReorderManager.start();
		} else {
			this.enableSequenceReordering = false;
			this.sequenceReorderManager = null;
		}
		this.payloadDecoder = payloadDecoder;
		this.hostId = hostId;
		this.onlineState = onlineState;

		this.sparkplugBExecutors = new ThreadPoolExecutor[DEFAULT_NUM_OF_THREADS];
		for (int i = 0; i < DEFAULT_NUM_OF_THREADS; i++) {
			final String uuid = UUID.randomUUID().toString().substring(0, 8);
			this.sparkplugBExecutors[i] = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
					new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
						@Override
						public Thread newThread(Runnable r) {
							final String threadName = String.format("%s-%s", "TahuHostCallback-", uuid);
							return new Thread(r, threadName);
						}
					});
		}
	}

	@Override
	public void shutdown() {
		logger.info("Shutting down TahuHostCallback");
		for (int i = 0; i < DEFAULT_NUM_OF_THREADS; i++) {
			try {
				sparkplugBExecutors[i].shutdownNow();
			} catch (Exception e) {
				logger.error("Failed to shutdown executor", e);
			}
		}
	}

	public void setMqttClients(Map<MqttServerName, TahuClient> tahuClients) {
		this.tahuClients = tahuClients;
	}

	public void setOnlineState(boolean onlineState) {
		this.onlineState = onlineState;
	}

	@Override
	public void messageArrived(MqttServerName server, MqttServerUrl url, MqttClientId clientId, String topic,
			MqttMessage message) {
		try {
			// What sent the message - and what type?

			TahuClient client = tahuClients.get(server);
			if (client == null) {
				logger.error("Message arrived on topic {} from unknown client {} on {}", topic, clientId, server);

				// Debug messages
				for (Entry<MqttServerName, TahuClient> entry : tahuClients.entrySet()) {
					logger.error("Failed - but found: {}", entry.getKey());
				}

				return;
			} else {
				logger.trace("Message arrived on topic {} from client {}", topic, clientId);
			}

			if (topic == null) {
				// Should never get here since we should only get messages on topics we subscribe to
				logger.error("Invalid null topic");
				return;
			}

			final String[] splitTopic = TopicUtil.getSplitTopic(topic);

			final long arrivedTime = System.nanoTime();
			if (topic.startsWith(SparkplugMeta.SPARKPLUG_B_TOPIC_PREFIX)) {
				if (splitTopic.length == 3 && splitTopic[1].equals("STATE")) {
					// This is a STATE message - handle as needed
					ObjectMapper mapper = new ObjectMapper();
					StatePayload statePayload = mapper.readValue(new String(message.getPayload()), StatePayload.class);
					if (hostId != null && !hostId.trim().isEmpty() && splitTopic[2].equals(hostId)) {
						// Correct the state if it is not correct
						if (!statePayload.isOnline() && onlineState) {
							logger.info(
									"This is a offline STATE message from {} - correcting with new online STATE message",
									splitTopic[2]);
							client.publishBirthMessage();
						} else if (statePayload.isOnline() && !onlineState) {
							logger.info(
									"This is a online STATE message from {} - correcting with new offline STATE message",
									splitTopic[2]);
							client.publishLwt(true);
						}
					}
				} else {
					// Get the proper executor
					String key = splitTopic[1] + "/" + splitTopic[3];
					int index = getThreadPoolExecutorIndex(key, DEFAULT_NUM_OF_THREADS);
					logger.debug("Adding Sparkplug B message to ThreadPoolExecutor {} :: {}", index,
							sparkplugBExecutors[index].getQueue().size());
					ThreadPoolExecutor executor = sparkplugBExecutors[index];

					if (enableSequenceReordering) {
						// Sequence reordering is required
						logger.trace("Sending the message on {} to the SequenceReorderManager", topic);
						sequenceReorderManager.handlePayload(this, executor, topic, splitTopic, message, server,
								clientId, arrivedTime);
					} else {
						executor.execute(() -> {
							try {
								// No sequence reordering required - just push the message through and handle the
								// Sparkplug B Payload
								logger.trace("Sending the message on {} directly to the TahuPayloadHandler", topic);
								new TahuPayloadHandler(eventHandler, commandPublisher, payloadDecoder)
										.handlePayload(topic, splitTopic, message, server, clientId);
							} catch (Throwable t) {
								logger.error("Failed to handle Sparkplug B message on topic {}", topic, t);
							} finally {
								// Update the message latency
								long latency = System.nanoTime() - arrivedTime;
								if (logger.isTraceEnabled()) {
									logger.trace("Updating message processing latency {}", latency);
								}
							}
						});
					}
				}
			} else {
				logger.debug("Received non-Sparkplug message on topic {}", topic);
			}
		} catch (Throwable t) {
			logger.error("Failed to handle message on topic {}", topic, t);
		}
	}

	/*
	 * Returns and index for the supplied key and number of ThreadPoolExecutors.
	 */
	private int getThreadPoolExecutorIndex(String key, int numOfThreadPoolExecutors) {
		return Math.abs(key.hashCode() % numOfThreadPoolExecutors);
	}

	@Override
	public void connectionLost(MqttServerName mqttServerName, MqttServerUrl url, MqttClientId clientId,
			Throwable cause) {
		logger.warn("Connection Lost to - {} :: {} :: {}", mqttServerName, url, clientId);
		eventHandler.onDisconnect(mqttServerName);

		if (cause != null) {
			// We don't need to see all of the connection lost callbacks for clients
			logger.error("Connection lost due to - {}", cause.getMessage(), cause);
		}

		logger.info("Clear out all connection counts to this MQTT Server");
		tahuClients.get(mqttServerName).clearConnectionCount();

		TahuClient tahuClient = tahuClients.get(mqttServerName);
//		edgeNodeManager.disconnectAllEdgeNodes(tahuClient);

		// Update the OFFLINE Engine Info tag for the client
//		updateEngineInfoDateTag(mqttServerName, DATE_OFFLINE);

		// Update the Primary Host State tag to OFFLINE
		String lwtTopic = tahuClients.get(mqttServerName).getLwtTopic();
		if (lwtTopic != null && lwtTopic.startsWith(SparkplugMeta.SPARKPLUG_TOPIC_HOST_STATE_PREFIX)) {
			String primaryHostId =
					lwtTopic.substring(SparkplugMeta.SPARKPLUG_TOPIC_HOST_STATE_PREFIX.length() + 1, lwtTopic.length());
			logger.debug("Setting Primary Host ID info tag for {} to offline", primaryHostId);
//			String clientTagPath = join(EngineGwHook.MQTT_CLIENTS_PATH, mqttServerName, "/",
//					EngineTag.PRIMARY_HOST_STATE, "/", primaryHostId, "/");
//			ModuleTagUtils.updateModuleTagValue(EngineSettings.getInstance().getContext(),
//					EngineSettings.getInstance().getManagedTagProvider(),
//					EngineSettings.getInstance().getManagedTagProviderName(), join(clientTagPath, "Payload"),
//					DataType.String, "OFFLINE");
		}

		if (tahuClient.getAutoReconnect()) {
			tahuClient.connect();
		}
	}

	@Override
	public void connectComplete(boolean reconnect, MqttServerName server, MqttServerUrl url, MqttClientId clientId) {
//		// Update the ONLINE Engine Info tag for the client
//		updateEngineInfoDateTag(server, DATE_ONLINE);
		eventHandler.onConnect(server);
	}

	private void updateEngineInfoDateTag(MqttServerName server, String tagName) {
//		ModuleTagUtils.updateModuleTagValue(EngineSettings.getInstance().getContext(),
//				EngineSettings.getInstance().getManagedTagProvider(),
//				EngineSettings.getInstance().getManagedTagProviderName(),
//				join(EngineGwHook.MQTT_CLIENTS_PATH, server, "/", tagName), DataType.DateTime, new Date());
	}
}
