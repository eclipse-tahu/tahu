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

package org.eclipse.tahu.host;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.tahu.exception.TahuErrorCode;
import org.eclipse.tahu.exception.TahuException;
import org.eclipse.tahu.host.api.MultiHostApplicationEventHandler;
import org.eclipse.tahu.host.manager.EdgeNodeManager;
import org.eclipse.tahu.host.manager.MetricManager;
import org.eclipse.tahu.host.manager.SparkplugDevice;
import org.eclipse.tahu.host.manager.SparkplugEdgeNode;
import org.eclipse.tahu.host.model.HostApplicationMetricMap;
import org.eclipse.tahu.host.model.HostMetric;
import org.eclipse.tahu.host.model.MessageContext;
import org.eclipse.tahu.message.PayloadDecoder;
import org.eclipse.tahu.message.model.DeviceDescriptor;
import org.eclipse.tahu.message.model.EdgeNodeDescriptor;
import org.eclipse.tahu.message.model.MessageType;
import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.Metric.MetricBuilder;
import org.eclipse.tahu.message.model.MetricDataType;
import org.eclipse.tahu.message.model.SparkplugBPayload;
import org.eclipse.tahu.message.model.SparkplugBPayload.SparkplugBPayloadBuilder;
import org.eclipse.tahu.message.model.SparkplugDescriptor;
import org.eclipse.tahu.message.model.Topic;
import org.eclipse.tahu.mqtt.MqttClientId;
import org.eclipse.tahu.mqtt.MqttServerName;
import org.eclipse.tahu.util.SparkplugUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TahuPayloadHandler {

	private static Logger logger = LoggerFactory.getLogger(TahuPayloadHandler.class.getName());

	private static Map<EdgeNodeDescriptor, Timer> rebirthTimers = new ConcurrentHashMap<>();

	private final MultiHostApplicationEventHandler eventHandler;

	private final CommandPublisher commandPublisher;

	private final PayloadDecoder<SparkplugBPayload> payloadDecoder;

	public TahuPayloadHandler(MultiHostApplicationEventHandler eventHandler, CommandPublisher commandPublisher,
			PayloadDecoder<SparkplugBPayload> payloadDecoder) {
		this.eventHandler = eventHandler;
		this.commandPublisher = commandPublisher;
		this.payloadDecoder = payloadDecoder;
	}

	public void handlePayload(String topicString, String[] splitTopic, MqttMessage message,
			MqttServerName mqttServerName, MqttClientId hostAppMqttClientId) {
		logger.trace("Handling payload on {}", topicString);

		Topic topic = null;
		try {
			if (splitTopic.length == 4) {
				topic = new Topic(splitTopic[0], splitTopic[1], splitTopic[3], MessageType.valueOf(splitTopic[2]));
			} else if (splitTopic.length == 5) {
				topic = new Topic(splitTopic[0], splitTopic[1], splitTopic[3], splitTopic[4],
						MessageType.valueOf(splitTopic[2]));
			} else {
				logger.error("Failed to handle the topic '{}'", topicString);
				return;
			}
		} catch (Exception e) {
			logger.error("Error parsing topic", e);
			return;
		}
		MessageType type = topic.getType();

		SparkplugBPayload payload = null;
		try {
			// Parse the payload
			payload = payloadDecoder.buildFromByteArray(message.getPayload(), HostApplicationMetricMap.getInstance()
					.getMetricDataTypeMap(topic.getEdgeNodeDescriptor(), topic.getSparkplugDescriptor()));
			logger.trace("On topic={}: Incoming payload: {}", topic, payload);
		} catch (Exception e) {
			logger.error("Failed to decode the payload", e);
			return;
		}

		if (type.isCommand()) {
			// This was an outbound command - ignore it
			logger.debug("Ignoring outbound command: {}", topicString);
			return;
		}

		// Extract the sequence number unless it is a node death certificate
		Long seqNum = null;
		if (!type.equals(MessageType.NDEATH)) {
			if (payload == null || payload.getSeq() == null) {
				logger.error("Invalid payload with topic={}: {}", topicString,
						payload == null ? "payload is null" : "sequence number is null");
				return;
			} else {
				seqNum = payload.getSeq();
				if (seqNum == null) {
					logger.error("Invalid payload missing sequence number: {}", topicString);
					return;
				}
			}
		}

		try {
			MessageContext messageContext = new MessageContext(mqttServerName, hostAppMqttClientId, topic, payload,
					message.getPayload() == null ? 0 : message.getPayload().length, seqNum == null ? -1 : seqNum);

			switch (type) {
				case NBIRTH:
					logger.debug("Handling NBIRTH from {}", topic.getSparkplugDescriptor());
					handleNodeBirth(mqttServerName, messageContext);
					break;
				case DBIRTH:
					logger.debug("Handling DBIRTH from {}", topic.getSparkplugDescriptor());
					handleDeviceBirth(mqttServerName, messageContext);
					break;
				case NDATA:
					logger.debug("Handling NDATA from {}", topic.getSparkplugDescriptor());
					handleNodeData(mqttServerName, messageContext);
					break;
				case DDATA:
					logger.debug("Handling DDATA from {}", topic.getSparkplugDescriptor());
					handleDeviceData(mqttServerName, messageContext);
					break;
				case NDEATH:
					logger.debug("Handling NDEATH from {}", topic.getSparkplugDescriptor());
					handleNodeDeath(mqttServerName, messageContext);
					break;
				case DDEATH:
					logger.debug("Handling DDEATH from {}", topic.getSparkplugDescriptor());
					handleDeviceDeath(mqttServerName, messageContext);
					break;

				default:
					logger.debug("Unknown message with type={} on topic={}", type, topic);
			}
		} catch (Exception e) {
			logger.error("Failed to handle payload on topic: {} with payload={}", topic, payload, e);
			return;
		}
	}

	protected void handleNodeBirth(MqttServerName mqttServerName, MessageContext messageContext) throws Exception {
		logger.debug("Processing NBIRTH from Edge Node {} with Seq# {}",
				messageContext.getTopic().getEdgeNodeDescriptor(), messageContext.getSeqNum());
		EdgeNodeDescriptor edgeNodeDescriptor = messageContext.getTopic().getEdgeNodeDescriptor();
		SparkplugEdgeNode sparkplugEdgeNode =
				EdgeNodeManager.getInstance().getSparkplugEdgeNode(messageContext.getTopic().getEdgeNodeDescriptor());
		if (sparkplugEdgeNode == null) {
			sparkplugEdgeNode = EdgeNodeManager.getInstance().addSparkplugEdgeNode(edgeNodeDescriptor,
					messageContext.getMqttServerName(), messageContext.getHostAppMqttClientId());
		} else {
			// Reset the metrics
			sparkplugEdgeNode.clearMetrics();
		}

		// Reset the alias map
		HostApplicationMetricMap hostApplicationMetricMap = HostApplicationMetricMap.getInstance();
		hostApplicationMetricMap.clear(sparkplugEdgeNode.getEdgeNodeDescriptor());

		// Set online
		sparkplugEdgeNode.setOnline(true, messageContext.getPayload().getTimestamp(),
				SparkplugUtil.getBdSequenceNumber(messageContext.getPayload()), messageContext.getSeqNum());

		eventHandler.onNodeBirthArrived(mqttServerName, edgeNodeDescriptor, messageContext.getMessage());
		eventHandler.onMessage(mqttServerName, edgeNodeDescriptor, messageContext.getMessage());
		for (Metric metric : messageContext.getPayload().getMetrics()) {
			if (metric.hasAlias()) {
				// Make sure the alias doesn't already exist
				if (hostApplicationMetricMap.aliasExists(edgeNodeDescriptor,
						messageContext.getTopic().getSparkplugDescriptor(), metric.getAlias())) {
					String errorMessage = "Not adding duplicated alias for edgeNode=" + edgeNodeDescriptor + " - alias="
							+ metric.getAlias() + " and metric name=" + metric.getName() + " - with existing alias for "
							+ hostApplicationMetricMap.getMetricName(edgeNodeDescriptor,
									messageContext.getTopic().getSparkplugDescriptor(), metric.getAlias());
					logger.error(errorMessage);

					requestRebirth(messageContext.getMqttServerName(), messageContext.getHostAppMqttClientId(),
							messageContext.getTopic().getEdgeNodeDescriptor());
					throw new TahuException(TahuErrorCode.INVALID_ARGUMENT, errorMessage);
				}
			}

			hostApplicationMetricMap.addMetric(edgeNodeDescriptor, edgeNodeDescriptor, metric.getName(), metric);

			// Update the cache and notify
			sparkplugEdgeNode.putMetric(metric.getName(), new HostMetric(metric, false));
			eventHandler.onBirthMetric(mqttServerName, edgeNodeDescriptor, metric);
		}
		eventHandler.onNodeBirthComplete(mqttServerName, edgeNodeDescriptor);
	}

	protected void handleDeviceBirth(MqttServerName mqttServerName, MessageContext messageContext) throws Exception {
		logger.debug("Processing DBIRTH from Device {} with Seq# {}",
				messageContext.getTopic().getSparkplugDescriptor(), messageContext.getSeqNum());
		EdgeNodeDescriptor edgeNodeDescriptor = messageContext.getTopic().getEdgeNodeDescriptor();
		DeviceDescriptor deviceDescriptor = (DeviceDescriptor) messageContext.getTopic().getSparkplugDescriptor();
		SparkplugEdgeNode sparkplugEdgeNode = EdgeNodeManager.getInstance().getSparkplugEdgeNode(edgeNodeDescriptor);
		SparkplugDevice sparkplugDevice =
				EdgeNodeManager.getInstance().getSparkplugDevice(edgeNodeDescriptor, deviceDescriptor);
		if (sparkplugDevice == null) {
			sparkplugDevice = EdgeNodeManager.getInstance().addSparkplugDevice(edgeNodeDescriptor, deviceDescriptor,
					messageContext.getPayload().getTimestamp());
		} else {
			sparkplugDevice.clearMetrics();
		}

		sparkplugEdgeNode.handleSeq(messageContext.getPayload().getSeq());

		// Set online
		sparkplugDevice.setOnline(true, messageContext.getPayload().getTimestamp());

		eventHandler.onDeviceBirthArrived(mqttServerName, deviceDescriptor, messageContext.getMessage());
		eventHandler.onMessage(mqttServerName, deviceDescriptor, messageContext.getMessage());
		HostApplicationMetricMap hostApplicationMetricMap = HostApplicationMetricMap.getInstance();
		for (Metric metric : messageContext.getPayload().getMetrics()) {
			if (metric.hasAlias()) {
				if (hostApplicationMetricMap.aliasExists(edgeNodeDescriptor, deviceDescriptor, metric.getAlias())) {
					String errorMessage = "Not adding duplicated alias for device=" + deviceDescriptor + " - alias="
							+ metric.getAlias() + " and metric name=" + metric.getName() + " - with existing alias for "
							+ hostApplicationMetricMap.getMetricName(edgeNodeDescriptor, deviceDescriptor,
									metric.getAlias());
					logger.error(errorMessage);

					requestRebirth(messageContext.getMqttServerName(), messageContext.getHostAppMqttClientId(),
							messageContext.getTopic().getEdgeNodeDescriptor());
					throw new TahuException(TahuErrorCode.INVALID_ARGUMENT, errorMessage);
				}
			}

			hostApplicationMetricMap.addMetric(edgeNodeDescriptor, deviceDescriptor, metric.getName(), metric);

			// Update the cache and notify
			sparkplugDevice.putMetric(metric.getName(), new HostMetric(metric, false));
			eventHandler.onBirthMetric(mqttServerName, deviceDescriptor, metric);
		}
		eventHandler.onDeviceBirthComplete(mqttServerName, deviceDescriptor);
	}

	protected void handleNodeData(MqttServerName mqttServerName, MessageContext messageContext) throws Exception {
		logger.debug("Processing NDATA from Edge Node {} with Seq# {}",
				messageContext.getTopic().getEdgeNodeDescriptor(), messageContext.getSeqNum());
		EdgeNodeDescriptor edgeNodeDescriptor = messageContext.getTopic().getEdgeNodeDescriptor();
		SparkplugEdgeNode sparkplugEdgeNode =
				EdgeNodeManager.getInstance().getSparkplugEdgeNode(messageContext.getTopic().getEdgeNodeDescriptor());
		if (sparkplugEdgeNode == null || !sparkplugEdgeNode.isOnline()) {
			requestRebirth(messageContext.getMqttServerName(), messageContext.getHostAppMqttClientId(),
					messageContext.getTopic().getEdgeNodeDescriptor());
			throw new TahuException(TahuErrorCode.INVALID_ARGUMENT,
					"Invalid state of the Sparkplug Edge Node when receiving a NDATA - "
							+ messageContext.getTopic().getSparkplugDescriptor() + " is offline");
		}

		sparkplugEdgeNode.handleSeq(messageContext.getPayload().getSeq());

		eventHandler.onNodeDataArrived(mqttServerName, edgeNodeDescriptor, messageContext.getMessage());
		eventHandler.onMessage(mqttServerName, edgeNodeDescriptor, messageContext.getMessage());
		for (Metric metric : messageContext.getPayload().getMetrics()) {
			if (!metric.hasName() && metric.hasAlias()) {
				metric.setName(HostApplicationMetricMap.getInstance().getMetricName(edgeNodeDescriptor,
						edgeNodeDescriptor, metric.getAlias()));
			}

			// Update the metric in the cache and notify
			sparkplugEdgeNode.updateValue(metric.getName(), metric.getValue());
			eventHandler.onDataMetric(mqttServerName, edgeNodeDescriptor, metric);
		}
		eventHandler.onNodeDataArrived(mqttServerName, edgeNodeDescriptor, messageContext.getMessage());
	}

	protected void handleDeviceData(MqttServerName mqttServerName, MessageContext messageContext) throws Exception {
		logger.debug("Processing DDATA from Device {} with Seq# {}", messageContext.getTopic().getSparkplugDescriptor(),
				messageContext.getSeqNum());
		EdgeNodeDescriptor edgeNodeDescriptor = messageContext.getTopic().getEdgeNodeDescriptor();
		DeviceDescriptor deviceDescriptor = (DeviceDescriptor) messageContext.getTopic().getSparkplugDescriptor();
		SparkplugEdgeNode sparkplugEdgeNode = EdgeNodeManager.getInstance().getSparkplugEdgeNode(edgeNodeDescriptor);
		SparkplugDevice sparkplugDevice =
				EdgeNodeManager.getInstance().getSparkplugDevice(edgeNodeDescriptor, deviceDescriptor);
		if (sparkplugDevice == null || !sparkplugEdgeNode.isOnline()) {
			requestRebirth(messageContext.getMqttServerName(), messageContext.getHostAppMqttClientId(),
					messageContext.getTopic().getEdgeNodeDescriptor());
			throw new TahuException(TahuErrorCode.INVALID_ARGUMENT,
					"Invalid state of the Sparkplug Device when receiving a DDATA - "
							+ messageContext.getTopic().getSparkplugDescriptor() + " is offline");
		}

		sparkplugEdgeNode.handleSeq(messageContext.getPayload().getSeq());

		eventHandler.onDeviceDataArrived(mqttServerName, deviceDescriptor, messageContext.getMessage());
		eventHandler.onMessage(mqttServerName, deviceDescriptor, messageContext.getMessage());
		for (Metric metric : messageContext.getPayload().getMetrics()) {
			if (!metric.hasName() && metric.hasAlias()) {
				metric.setName(HostApplicationMetricMap.getInstance().getMetricName(edgeNodeDescriptor,
						deviceDescriptor, metric.getAlias()));
			}

			// Update the metric in the cache and notify
			sparkplugDevice.updateValue(metric.getName(), metric.getValue());
			eventHandler.onDataMetric(mqttServerName, deviceDescriptor, metric);
		}
		eventHandler.onDeviceDataComplete(mqttServerName, deviceDescriptor);
	}

	protected void handleNodeDeath(MqttServerName mqttServerName, MessageContext messageContext) {
		Long incomingBdSeqNum = -1L;
		EdgeNodeDescriptor edgeNodeDescriptor = messageContext.getTopic().getEdgeNodeDescriptor();
		try {
			SparkplugEdgeNode sparkplugEdgeNode =
					EdgeNodeManager.getInstance().getSparkplugEdgeNode(edgeNodeDescriptor);
			incomingBdSeqNum = SparkplugUtil.getBdSequenceNumber(messageContext.getPayload());
			if (sparkplugEdgeNode != null && incomingBdSeqNum != null) {
				if (sparkplugEdgeNode.isOnline()) {
					long birthBdSeqNum = sparkplugEdgeNode.getBirthBdSeqNum();
					if (birthBdSeqNum == incomingBdSeqNum) {
						eventHandler.onNodeDeath(mqttServerName, edgeNodeDescriptor, messageContext.getMessage());
						eventHandler.onMessage(mqttServerName, edgeNodeDescriptor, messageContext.getMessage());
						staleTags(mqttServerName, edgeNodeDescriptor, sparkplugEdgeNode);
						sparkplugEdgeNode.setOnline(false, messageContext.getPayload().getTimestamp(), incomingBdSeqNum,
								null);
						for (SparkplugDevice sparkplugDevice : sparkplugEdgeNode.getSparkplugDevices().values()) {
							staleTags(mqttServerName, sparkplugDevice.getDeviceDescrptor(), sparkplugDevice);
							sparkplugDevice.setOnline(false, messageContext.getPayload().getTimestamp());
						}
						eventHandler.onNodeDeathComplete(mqttServerName, edgeNodeDescriptor);
					} else {
						logger.error(
								"Edge Node bdSeq number mismatch on incoming NDEATH from {} - received {}, expected {} - ignoring NDEATH",
								edgeNodeDescriptor, incomingBdSeqNum, birthBdSeqNum);
					}
				} else {
					logger.error("Edge Node '{}' is not online - ignoring NDEATH", edgeNodeDescriptor);
				}
			} else {
				logger.error("Unable to find Edge Node or current bdSeq number for NDEATH from {} - ignoring NDEATH",
						messageContext.getTopic().getEdgeNodeDescriptor());
			}
		} catch (Exception e) {
			logger.error("Sparkplug BD sequence number from {} is missing - ignoring NDEATH", edgeNodeDescriptor);
		}
	}

	protected void handleDeviceDeath(MqttServerName mqttServerName, MessageContext messageContext)
			throws TahuException {
		EdgeNodeDescriptor edgeNodeDescriptor = messageContext.getTopic().getEdgeNodeDescriptor();
		DeviceDescriptor deviceDescriptor = (DeviceDescriptor) messageContext.getTopic().getSparkplugDescriptor();
		SparkplugEdgeNode sparkplugEdgeNode = EdgeNodeManager.getInstance().getSparkplugEdgeNode(edgeNodeDescriptor);
		SparkplugDevice sparkplugDevice =
				EdgeNodeManager.getInstance().getSparkplugDevice(edgeNodeDescriptor, deviceDescriptor);
		if (sparkplugDevice == null || !sparkplugEdgeNode.isOnline() || !sparkplugDevice.isOnline()) {
			logger.error("Invalid state of the Sparkplug Device when receiving a DDEATH - "
					+ messageContext.getTopic().getSparkplugDescriptor() + " is offline - ignoring DDEATH");
			return;
		}

		sparkplugEdgeNode.handleSeq(messageContext.getPayload().getSeq());

		if (sparkplugEdgeNode.isOnline() && sparkplugDevice.isOnline()) {
			eventHandler.onDeviceDeath(mqttServerName, deviceDescriptor, messageContext.getMessage());
			eventHandler.onMessage(mqttServerName, deviceDescriptor, messageContext.getMessage());
			staleTags(mqttServerName, deviceDescriptor, sparkplugDevice);
			sparkplugDevice.setOnline(false, messageContext.getPayload().getTimestamp());
			eventHandler.onDeviceDeathComplete(mqttServerName, deviceDescriptor);
		} else {
			logger.error("Online requirements not met for {} - edgeNode={} and device={} - ignoring DDEATH",
					deviceDescriptor, sparkplugEdgeNode.isOnline() ? "online" : "offline",
					sparkplugDevice.isOnline() ? "online" : "offline");
		}
	}

	private void staleTags(MqttServerName mqttServerName, SparkplugDescriptor sparkplugDescriptor,
			MetricManager metricManager) {
		// Stale all tags associated with this Edge Node
		Set<String> metricNames = metricManager.getMetricNames();
		Iterator<String> it = metricNames.iterator();
		while (it.hasNext()) {
			String metricName = it.next();

			// Update the cache and notify
			metricManager.setStale(metricName, true);
			eventHandler.onStale(mqttServerName, sparkplugDescriptor, metricManager.getMetric(metricName));
		}
	}

	public void requestRebirth(MqttServerName mqttServerName, MqttClientId hostAppMqttClientId,
			EdgeNodeDescriptor edgeNodeDescriptor) {
		requestRebirth(mqttServerName, hostAppMqttClientId, edgeNodeDescriptor, null);
	}

	public void requestRebirth(MqttServerName mqttServerName, MqttClientId hostAppMqttClientId,
			EdgeNodeDescriptor edgeNodeDescriptor, SparkplugEdgeNode sparkplugEdgeNode) {
		try {
			Timer rebirthDelayTimer = rebirthTimers.get(edgeNodeDescriptor);
			if (rebirthDelayTimer == null) {
				logger.debug("Requesting Rebirth from {}", edgeNodeDescriptor);
				rebirthDelayTimer = new Timer();
				rebirthTimers.put(edgeNodeDescriptor, rebirthDelayTimer);
				rebirthDelayTimer.schedule(new RebirthDelayTask(edgeNodeDescriptor), 5000);

				// Request a rebirth
				SparkplugBPayload cmdPayload = new SparkplugBPayloadBuilder().setTimestamp(new Date())
						.addMetric(
								new MetricBuilder("Node Control/Rebirth", MetricDataType.Boolean, true).createMetric())
						.createPayload();

				Topic cmdTopic = new Topic("spBv1.0", edgeNodeDescriptor, MessageType.NCMD);
				if (sparkplugEdgeNode != null) {
					// Set the Edge Node offline
					sparkplugEdgeNode.forceOffline(new Date());

					if (mqttServerName != null && sparkplugEdgeNode.getMqttServerName() != null
							&& mqttServerName.equals(sparkplugEdgeNode.getMqttServerName())) {
						logger.debug("On Rebirth request - Current Engine MQTT Server is unchanged: {}",
								mqttServerName);
					} else {
						logger.debug("On Rebirth request - MQTT Server has changed: new={}, old={}", mqttServerName,
								sparkplugEdgeNode.getMqttServerName());
					}
					if (hostAppMqttClientId != null && sparkplugEdgeNode.getHostAppMqttClientId() != null
							&& hostAppMqttClientId.equals(sparkplugEdgeNode.getHostAppMqttClientId())) {
						logger.debug("On Rebirth request - Current Engine MQTT Client ID is unchanged: {}",
								hostAppMqttClientId);
					} else {
						logger.debug("On Rebirth request - MQTT Client ID has changed: new={}, old={}",
								hostAppMqttClientId, sparkplugEdgeNode.getHostAppMqttClientId());
					}

					// Update the current Engine MQTT Server name and Client ID
					sparkplugEdgeNode.setMqttServerName(mqttServerName);
					sparkplugEdgeNode.setHostAppMqttClientId(hostAppMqttClientId);

					publishCommand(mqttServerName, cmdTopic, cmdPayload);
				} else {
					logger.debug("Current Engine MQTT Server Name for unknown Edge Node: {}", mqttServerName);
					logger.debug("Current Engine MQTT Client ID for unknown Edge Node: {}", hostAppMqttClientId);
					publishCommand(mqttServerName, cmdTopic, cmdPayload);
				}
			} else {
				logger.debug("Not requesting Rebirth since we have in the last 5 seconds");
			}
		} catch (Exception e) {
			logger.error("Failed to create Rebirth request", e);
			return;
		}
	}

	/**
	 * A TimerTask subclass for timers on issued rebirth requests.
	 */
	private class RebirthDelayTask extends TimerTask {
		private EdgeNodeDescriptor edgeNodeDescriptor;

		public RebirthDelayTask(EdgeNodeDescriptor edgeNodeDescriptor) {
			this.edgeNodeDescriptor = edgeNodeDescriptor;
		}

		public void run() {
			if (rebirthTimers.get(edgeNodeDescriptor) != null) {
				rebirthTimers.get(edgeNodeDescriptor).cancel();
				rebirthTimers.remove(edgeNodeDescriptor);
			}
		}
	}

	private void publishCommand(MqttServerName mqttServerName, Topic topic, SparkplugBPayload payload)
			throws Exception {
		commandPublisher.publishCommand(mqttServerName, topic, payload);
	}
}
