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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.tahu.exception.TahuErrorCode;
import org.eclipse.tahu.exception.TahuException;
import org.eclipse.tahu.host.api.MultiHostApplicationEventHandler;
import org.eclipse.tahu.host.seq.SequenceReorderManager;
import org.eclipse.tahu.message.PayloadDecoder;
import org.eclipse.tahu.message.SparkplugBPayloadEncoder;
import org.eclipse.tahu.message.model.SparkplugBPayload;
import org.eclipse.tahu.message.model.SparkplugMeta;
import org.eclipse.tahu.message.model.Topic;
import org.eclipse.tahu.model.MqttServerDefinition;
import org.eclipse.tahu.mqtt.MqttOperatorDefs;
import org.eclipse.tahu.mqtt.MqttServerName;
import org.eclipse.tahu.mqtt.RandomStartupDelay;
import org.eclipse.tahu.mqtt.TahuClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HostApplication implements CommandPublisher {

	private static Logger logger = LoggerFactory.getLogger(HostApplication.class.getName());

	private static int MAX_INFLIGHT_MESSAGES = 500;

	private final String hostId;
	private final RandomStartupDelay randomStartupDelay;
	private final String stateTopic;
	private final List<String> sparkplugSubscriptons;
	private final TahuHostCallback tahuHostCallback;
	private final List<MqttServerDefinition> mqttServerDefinitions;
	private final Map<MqttServerName, TahuClient> tahuClients = new HashMap<>();

	public HostApplication(MultiHostApplicationEventHandler eventHandler, String hostId,
			List<String> sparkplugSubscriptons, List<MqttServerDefinition> mqttServerDefinitions,
			RandomStartupDelay randomStartupDelay, PayloadDecoder<SparkplugBPayload> payloadDecoder,
			boolean onlineState) {
		logger.info("Creating the Host Application");

		if (hostId != null) {
			this.hostId = hostId;
			this.stateTopic = SparkplugMeta.SPARKPLUG_TOPIC_HOST_STATE_PREFIX + "/" + hostId;
		} else {
			this.hostId = null;
			this.stateTopic = null;
		}
		this.sparkplugSubscriptons = sparkplugSubscriptons;
		this.mqttServerDefinitions = mqttServerDefinitions;
		this.randomStartupDelay = randomStartupDelay;

		SequenceReorderManager sequenceReorderManager = SequenceReorderManager.getInstance();
		sequenceReorderManager.init(eventHandler, this, payloadDecoder, 5000L);
		this.tahuHostCallback =
				new TahuHostCallback(eventHandler, this, sequenceReorderManager, payloadDecoder, hostId, onlineState);
	}

	public HostApplication(MultiHostApplicationEventHandler eventHandler, String hostId,
			List<String> sparkplugSubscriptons, TahuHostCallback tahuHostCallback,
			Map<MqttServerName, TahuClient> tahuClients, RandomStartupDelay randomStartupDelay) {
		logger.info("Creating the Host Application");

		if (hostId != null && !hostId.trim().isEmpty()) {
			this.hostId = hostId;
			this.stateTopic = SparkplugMeta.SPARKPLUG_TOPIC_HOST_STATE_PREFIX + "/" + hostId;
		} else {
			this.hostId = null;
			this.stateTopic = null;
		}

		this.sparkplugSubscriptons = sparkplugSubscriptons;
		this.tahuHostCallback = tahuHostCallback;
		this.mqttServerDefinitions = null;
		this.tahuClients.putAll(tahuClients);
		this.randomStartupDelay = randomStartupDelay;
	}

	public void start(boolean onlineState) {
		if (mqttServerDefinitions != null) {
			for (MqttServerDefinition mqttServerDefinition : mqttServerDefinitions) {
				logger.debug("Starting up the MQTT Client to {}", mqttServerDefinition.getMqttServerName());
				TahuClient tahuClient = tahuClients.get(mqttServerDefinition.getMqttServerName());
				if (tahuClient == null) {
					tahuClient = new TahuClient(mqttServerDefinition.getMqttClientId(),
							mqttServerDefinition.getMqttServerName(), mqttServerDefinition.getMqttServerUrl(),
							mqttServerDefinition.getUsername(), mqttServerDefinition.getPassword(), true,
							mqttServerDefinition.getKeepAliveTimeout(), tahuHostCallback, randomStartupDelay,
							onlineState, true, stateTopic, null, true, stateTopic, null, MqttOperatorDefs.QOS1, true);
				}

				// Add it to the Map
				tahuClients.put(mqttServerDefinition.getMqttServerName(), tahuClient);
			}
		}

		// Start the clients
		for (TahuClient client : tahuClients.values()) {
			startClient(client);
		}

		logger.debug("MQTT Clients Started. Connection and subscriptions not verified yet");
	}

	private void startClient(TahuClient tahuClient) {
		tahuClient.setMaxInflightMessages(MAX_INFLIGHT_MESSAGES);
		tahuHostCallback.setMqttClients(tahuClients);

		try {
			tahuClient.setAutoReconnect(true);
			tahuClient.connect();

			// Subscribe to our own STATE topic
			if (stateTopic != null) {
				logger.debug("PrimaryHostId is set. Subscribing on {}", stateTopic);
				int grantedQos = tahuClient.subscribe(stateTopic, MqttOperatorDefs.QOS1);
				if (grantedQos != 1) {
					logger.error("Failed to subscribe to '{}'", stateTopic);
					return;
				}
			}

			for (String subscriptionTopic : sparkplugSubscriptons) {
				// Subscribe to the Sparkplug namespace(s)
				logger.debug("Subscribing on {}", subscriptionTopic);
				int grantedQos = tahuClient.subscribe(subscriptionTopic, MqttOperatorDefs.QOS0);
				if (grantedQos != 0) {
					logger.error("Failed to subscribe to '{}'", subscriptionTopic);
					return;
				}
			}
		} catch (Exception e) {
			logger.error("Failed to start client {} connecting to {}", tahuClient.getClientId(),
					tahuClient.getMqttServerUrl(), e);
			return;
		}
	}

	public void shutdown() {
		for (TahuClient tahuClient : tahuClients.values()) {
			if (tahuClient != null) {
				String connectionId = new StringBuilder().append(tahuClient.getMqttServerUrl()).append(" :: ")
						.append(tahuClient.getClientId()).toString();
				try {
					// Unsubscribe
					// removeMqttClientSubscriptions(tahuClient, unsubscribe);

					if (stateTopic != null) {
						// Clean up STATE subscriptions
						logger.debug("Unsubscribing from {}", stateTopic);
						tahuClient.unsubscribe(stateTopic);
					}

					for (String subscriptionTopic : sparkplugSubscriptons) {
						// Clean up the Sparkplug subscription(s)
						logger.debug("Unsubscribing from {}", subscriptionTopic);
						tahuClient.unsubscribe(subscriptionTopic);
					}

					// Shut down the client after the MQTT client is disconnected to prevent RejectedExecutionExceptions
					tahuHostCallback.shutdown();

					// Shut down the MQTT client
					tahuClient.setAutoReconnect(false);
					logger.info("Attempting disconnect {}", connectionId);
					tahuClient.disconnect(100, 100, true, true);
					logger.info("Successfully disconnected {}", connectionId);

					// Set the Edge Nodes associated with this client offline
//					edgeNodeManager.setAllEdgeNodesOffline(mqttServerName);
				} catch (Exception e) {
					logger.error("Error shutting down {}", connectionId, e);
				} finally {
					tahuClient = null;
				}
			} else {
				logger.trace("Cannot shutdown null client");
			}
		}
	}

	/**
	 * Whether or not the host should be marked as online This should be set to false if the host is not ready to
	 * receive Sparkplug messages
	 *
	 * @param onlineState the state to set
	 */
	public void setOnlineState(boolean onlineState) {
		tahuHostCallback.setOnlineState(onlineState);
		for (TahuClient tahuClient : tahuClients.values()) {
			tahuClient.setOnlineState(onlineState);
		}
	}

	public String getHostId() {
		return hostId;
	}

	@Override
	public void publishCommand(Topic topic, SparkplugBPayload payload) throws Exception {
		for (MqttServerName mqttServerName : tahuClients.keySet()) {
			publishCommand(mqttServerName, topic, payload);
		}
	}

	@Override
	public void publishCommand(MqttServerName mqttServerName, Topic topic, SparkplugBPayload payload) throws Exception {
		TahuClient tahuClient = tahuClients.get(mqttServerName);
		if (tahuClient != null && tahuClient.isConnected()) {
			SparkplugBPayloadEncoder encoder = new SparkplugBPayloadEncoder();
			byte[] bytes = encoder.getBytes(payload, true);
			tahuClient.publish(topic.toString(), bytes, MqttOperatorDefs.QOS0, false);
		} else {
			throw new TahuException(TahuErrorCode.INITIALIZATION_ERROR,
					"The Tahu Client is not connected - not publishing command on topic=" + topic);
		}
	}
}
