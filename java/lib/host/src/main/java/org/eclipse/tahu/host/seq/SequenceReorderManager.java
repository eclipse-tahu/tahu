/********************************************************************************
 * Copyright (c) 2022-2025 Cirrus Link Solutions and others
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

package org.eclipse.tahu.host.seq;

import java.util.Calendar;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.tahu.SparkplugParsingException;
import org.eclipse.tahu.host.CommandPublisher;
import org.eclipse.tahu.host.TahuHostCallback;
import org.eclipse.tahu.host.TahuPayloadHandler;
import org.eclipse.tahu.host.api.MultiHostApplicationEventHandler;
import org.eclipse.tahu.host.manager.EdgeNodeManager;
import org.eclipse.tahu.host.manager.SparkplugEdgeNode;
import org.eclipse.tahu.host.model.HostApplicationMetricMap;
import org.eclipse.tahu.message.PayloadDecoder;
import org.eclipse.tahu.message.model.EdgeNodeDescriptor;
import org.eclipse.tahu.message.model.MessageType;
import org.eclipse.tahu.message.model.SparkplugBPayload;
import org.eclipse.tahu.message.model.Topic;
import org.eclipse.tahu.mqtt.MqttClientId;
import org.eclipse.tahu.mqtt.MqttServerName;
import org.eclipse.tahu.util.TopicUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SequenceReorderManager {

	private static Logger logger = LoggerFactory.getLogger(SequenceReorderManager.class.getName());

	private static SequenceReorderManager instance;

	private final static long SEQUENCE_MONITOR_TIMER = 1000L;

	private final Map<EdgeNodeDescriptor, SequenceReorderMap> edgeNodeMap;

	private final Object edgeNodeMapLock = new Object();

	private Timer timer;

	private MultiHostApplicationEventHandler eventHandler;

	private CommandPublisher commandPublisher;

	private PayloadDecoder<SparkplugBPayload> payloadDecoder;

	private Long timeout;

	public SequenceReorderManager() {
		this.edgeNodeMap = new ConcurrentHashMap<>();
	}

	public static SequenceReorderManager getInstance() {
		if (instance == null) {
			instance = new SequenceReorderManager();
		}
		return instance;
	}

	public void init(MultiHostApplicationEventHandler eventHandler, CommandPublisher commandPublisher,
			PayloadDecoder<SparkplugBPayload> payloadDecoder, Long timeout) {
		if (eventHandler != null && timeout != null) {
			this.eventHandler = eventHandler;
			this.commandPublisher = commandPublisher;
			this.payloadDecoder = payloadDecoder;
			this.timeout = timeout;
		} else {
			logger.error("Not re-initializing the SequenceReorderManager timer");
		}
	}

	public void start() {
		TimerTask monitorTask = new TimerTask() {
			public void run() {
				synchronized (edgeNodeMapLock) {
					edgeNodeMap.values().forEach(sequenceReorderMap -> {
						try {
							if (!sequenceReorderMap.isEmpty()) {
								Calendar calendar = Calendar.getInstance();
								calendar.add(Calendar.MILLISECOND, (int) (timeout * -1));
								if (sequenceReorderMap.getLastUpdateTime().before(calendar.getTime())) {
									// Timed out
									logger.info("Timeout while reording sequence numbers on {} with {} in queue",
											sequenceReorderMap.getEdgeNodeDescriptor(), sequenceReorderMap.size());
									SequenceReorderContext sequenceReorderContext =
											sequenceReorderMap.getExpiredSequenceReorderContext(timeout);
									if (sequenceReorderContext != null) {
										TahuPayloadHandler handler =
												new TahuPayloadHandler(eventHandler, commandPublisher, payloadDecoder);
										SparkplugEdgeNode edgeNode = EdgeNodeManager.getInstance()
												.getSparkplugEdgeNode(sequenceReorderMap.getEdgeNodeDescriptor());

										// Reset the map as all values are now invalid
										sequenceReorderMap.reset();

										if (edgeNode != null) {
											logger.info("Requesting a rebirth from known edge node {}",
													sequenceReorderMap.getEdgeNodeDescriptor());
											edgeNode.setHostAppMqttClientId(
													sequenceReorderContext.getHostAppMqttClientId());
											edgeNode.setMqttServerName(sequenceReorderContext.getMqttServerName());
											handler.requestRebirth(sequenceReorderContext.getMqttServerName(),
													sequenceReorderContext.getHostAppMqttClientId(),
													sequenceReorderMap.getEdgeNodeDescriptor(), edgeNode);
										} else {
											logger.info("Requesting a rebirth from unknown edge node {}",
													sequenceReorderMap.getEdgeNodeDescriptor());
											handler.requestRebirth(sequenceReorderContext.getMqttServerName(),
													sequenceReorderContext.getHostAppMqttClientId(),
													sequenceReorderMap.getEdgeNodeDescriptor());
										}
									}
								}
							}
						} catch (Exception e) {
							logger.error("Failed to handle reorder entry in monitor", e);
						}
					});
				}
			}
		};
		timer = new Timer("SequenceMonitorTimer");
		timer.scheduleAtFixedRate(monitorTask, SEQUENCE_MONITOR_TIMER, SEQUENCE_MONITOR_TIMER);
	}

	public void stop() {
		if (timer != null) {
			timer.cancel();
			timer = null;
		}
	}

	/**
	 * This handles a {@link SparkplugBPayload} when sequence number reordering is enabled. This method will buffer
	 * messages as they flow into MQTT Engine and reorder based on sequence numbers within a given timeout period.
	 * 
	 * @param engineCallback
	 * @param executor
	 * @param settingsAccessor
	 * @param edgeNodesAccessor
	 * @param topicString
	 * @param splitTopic
	 * @param message
	 * @param mqttServerName
	 * @param mqttClientId
	 * @param arrivedTime
	 * @throws Exception
	 */
	public void handlePayload(TahuHostCallback tahuHostCallback, ThreadPoolExecutor executor, final String topicString,
			final String[] splitTopic, final MqttMessage message, final MqttServerName mqttServerName,
			final MqttClientId mqttClientId, final long arrivedTime) throws Exception {

		// Get the Topic and MessageType
		Topic topic;
		try {
			topic = TopicUtil.parseTopic(splitTopic);
		} catch (SparkplugParsingException e) {
			logger.error("Error parsing topic", e);
			return;
		}
		MessageType messageType = topic.getType();

		// Early return for commands
		if (messageType == MessageType.NCMD || messageType == MessageType.DCMD) {
			return;
		}

		// Parse the payload
		SparkplugBPayload payload = payloadDecoder.buildFromByteArray(message.getPayload(), HostApplicationMetricMap
				.getInstance().getMetricDataTypeMap(topic.getEdgeNodeDescriptor(), topic.getSparkplugDescriptor()));
		logger.trace("Incoming payload: {}", payload);

		synchronized (edgeNodeMapLock) {
			// See if the Edge Node is known and add if not
			EdgeNodeDescriptor edgeNodeDescriptor = new EdgeNodeDescriptor(topic.getGroupId(), topic.getEdgeNodeId());
			SequenceReorderMap sequenceReorderMap =
					edgeNodeMap.computeIfAbsent(edgeNodeDescriptor, (k) -> new SequenceReorderMap(edgeNodeDescriptor));

			if (topic.isType(MessageType.NBIRTH)) {
				// Reset the expected sequence number to zero
				logger.debug("Resetting sequenceReorderMap on NBIRTH for {}", edgeNodeDescriptor);
				sequenceReorderMap.resetSeqNum();
			} else if (topic.isType(MessageType.NDEATH)) {
				// Handle NDEATH immediately and return
				handleMessage(tahuHostCallback, executor, new SequenceReorderContext(topicString, topic, message,
						payload, messageType, mqttServerName, mqttClientId, arrivedTime));
				return;
			} else if (topic.isType(MessageType.NCMD) || topic.isType(MessageType.DCMD)) {
				// Ignition NCMD and DCMD
				return;
			}

			// See if this is the next expected sequence number
			boolean passedSeqNumCheck = false;
			if (payload == null || payload.getSeq() == null) {
				logger.warn("Invalid payload arrived on topic={} with {}", topic,
						payload == null
								? "'payload is null'"
								: payload.getSeq() == null
										? "'payload sequence number is null'"
										: "sequence number is present - shouldn't have gotten here");
			} else {
				passedSeqNumCheck = sequenceReorderMap.liveSeqNumCheck(payload.getSeq());
			}

			if (passedSeqNumCheck) {
				// Set the session state
				if (topic.isType(MessageType.NBIRTH)) {
					sequenceReorderMap.prune(payload.getTimestamp());
				}

				// This is the next expected message - process it
				logger.debug("Handling real time message on {} with seqNum={}", topicString, payload.getSeq());
				handleMessage(tahuHostCallback, executor, new SequenceReorderContext(topicString, topic, message,
						payload, messageType, mqttServerName, mqttClientId, arrivedTime));

				// Now check to see if there are other messages to process
				if (!sequenceReorderMap.isEmpty()) {
					boolean done = false;
					long nextSeqNum = getNextSeqNum(payload.getSeq());
					while (!done && !sequenceReorderMap.isEmpty()) {
						SequenceReorderContext sequenceReorderContext =
								sequenceReorderMap.storedSeqNumCheck(nextSeqNum);
						if (sequenceReorderContext != null) {
							// This is the next expected message - publish it
							logger.debug("Handling stored message on {} with seqNum={}", topicString, nextSeqNum);
							handleMessage(tahuHostCallback, executor, new SequenceReorderContext(
//											sequenceReorderContext.getSettingsAccessor(),
//											sequenceReorderContext.getEdgeNodesAccessor(),
									sequenceReorderContext.getTopicString(), sequenceReorderContext.getTopic(),
									sequenceReorderContext.getMessage(), sequenceReorderContext.getPayload(),
									sequenceReorderContext.getMessageType(), sequenceReorderContext.getMqttServerName(),
									sequenceReorderContext.getHostAppMqttClientId(),
									sequenceReorderContext.getArrivedTime()));
							nextSeqNum = getNextSeqNum(nextSeqNum);
						} else {
							logger.debug("Failed to find SequenceReorderContext for {} - moving on", nextSeqNum);
							done = true;
						}
					}
				}
			} else {
				// This is not the next expected message - store it after handling session state
				logger.debug("Storing message on {} due to out of sequence message with seqNum={} - was expecting {}",
						topicString, payload.getSeq(), sequenceReorderMap.getNextExpectedSeqNum());
				SequenceReorderContext sequenceReorderContext = new SequenceReorderContext(topicString, topic, message,
						payload, messageType, mqttServerName, mqttClientId, arrivedTime);
				sequenceReorderMap.put(payload.getSeq(), sequenceReorderContext);
			}
		}
	}

	/**
	 * Removes and Edge Node from the {@link SequenceReorderManager}. This should be used any time an Edge Node goes
	 * offline.
	 * 
	 * @param edgeNodeDescriptor the {@link EdgeNodeDescriptor} of the Edge Node to remove
	 */
	public void removeEdgeNode(EdgeNodeDescriptor edgeNodeDescriptor) {
		synchronized (edgeNodeMapLock) {
			edgeNodeMap.remove(edgeNodeDescriptor);
		}
	}

	private long getNextSeqNum(long currentSeqNum) {
		long nextSeqNum = currentSeqNum + 1;
		if (nextSeqNum == 256) {
			nextSeqNum = 0;
		}
		return nextSeqNum;
	}

	private void handleMessage(TahuHostCallback tahuHostCallback, ThreadPoolExecutor executor,
			SequenceReorderContext sequenceReorderContext) {
		executor.execute(() -> {
			try {
				// Handle the SparkplugBPayload
				new TahuPayloadHandler(eventHandler, commandPublisher, payloadDecoder).handlePayload(
						sequenceReorderContext.getTopicString(), sequenceReorderContext.getSplitTopic(),
						sequenceReorderContext.getMessage(), sequenceReorderContext.getMqttServerName(),
						sequenceReorderContext.getHostAppMqttClientId());

			} catch (Throwable t) {
				logger.error("Failed to handle Sparkplug B message on topic {} - requesting rebirth",
						sequenceReorderContext.getTopic(), t);
				new TahuPayloadHandler(eventHandler, commandPublisher, payloadDecoder).requestRebirth(
						sequenceReorderContext.getMqttServerName(), sequenceReorderContext.getHostAppMqttClientId(),
						sequenceReorderContext.getTopic().getEdgeNodeDescriptor());
			} finally {
				// Update the message latency
				long latency = System.nanoTime() - sequenceReorderContext.getArrivedTime();
				if (logger.isTraceEnabled()) {
					logger.trace("Updating message processing latency {}", latency);
				}
			}
		});
	}
}
