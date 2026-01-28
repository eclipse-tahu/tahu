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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.eclipse.tahu.exception.TahuException;
import org.eclipse.tahu.host.api.MultiHostApplicationEventHandler;
import org.eclipse.tahu.message.SparkplugBPayloadDecoder;
import org.eclipse.tahu.message.model.DeviceDescriptor;
import org.eclipse.tahu.message.model.EdgeNodeDescriptor;
import org.eclipse.tahu.message.model.Message;
import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.SparkplugDescriptor;
import org.eclipse.tahu.message.model.SparkplugMeta;
import org.eclipse.tahu.model.MqttServerDefinition;
import org.eclipse.tahu.mqtt.MqttClientId;
import org.eclipse.tahu.mqtt.MqttServerName;
import org.eclipse.tahu.mqtt.MqttServerUrl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkplugHostApplication implements MultiHostApplicationEventHandler {

	private static Logger logger = LoggerFactory.getLogger(SparkplugHostApplication.class.getName());

	private static final String COMMAND_LISTENER_DIRECTORY = "/tmp/commands";
	private static final long COMMAND_LISTENER_POLL_RATE = 50L;

	private static final String HOST_ID = "IamHost";
	private static final String MQTT_SERVER_NAME_1 = "Mqtt Server One";
	private static final String MQTT_CLIENT_ID_1 = "Tahu_Host_Application";
	private static final String MQTT_SERVER_URL_1 = "tcp://localhost:1883";
	private static final String USERNAME_1 = "admin";
	private static final String PASSWORD_1 = "changeme";
	private static final String MQTT_SERVER_NAME_2 = "Mqtt Server Two";
	private static final String MQTT_CLIENT_ID_2 = "Tahu_Host_Application";
	private static final String MQTT_SERVER_URL_2 = "tcp://localhost:1884";
	private static final String USERNAME_2 = null;
	private static final String PASSWORD_2 = null;
	private static final int KEEP_ALIVE_TIMEOUT = 30;

	private CommandListener commandListener;
	private HostApplication hostApplication;

	private static final List<MqttServerDefinition> mqttServerDefinitions = new ArrayList<>();

	public static void main(String[] arg) {
		try {
			mqttServerDefinitions.add(new MqttServerDefinition(new MqttServerName(MQTT_SERVER_NAME_1),
					new MqttClientId(MQTT_CLIENT_ID_1, false), new MqttServerUrl(MQTT_SERVER_URL_1), USERNAME_1,
					PASSWORD_1, KEEP_ALIVE_TIMEOUT, null));
//			mqttServerDefinitions.add(new MqttServerDefinition(new MqttServerName(MQTT_SERVER_NAME_2),
//					new MqttClientId(MQTT_CLIENT_ID_2, false), new MqttServerUrl(MQTT_SERVER_URL_2), USERNAME_2,
//					PASSWORD_2, KEEP_ALIVE_TIMEOUT, null));

			System.out.println("Starting the Sparkplug Host Application");
			System.out.println("\tSparkplug Host Application ID: " + HOST_ID);
			System.out.println("\tKeep Alive Timeout: " + KEEP_ALIVE_TIMEOUT);
			System.out.println("\tCommand Listener Directory: " + COMMAND_LISTENER_DIRECTORY);
			System.out.println("\tCommand Listener Poll Rate: " + COMMAND_LISTENER_POLL_RATE);

			for (MqttServerDefinition mqttServerDefinition : mqttServerDefinitions) {
				System.out.println("\tMQTT Server Name: " + mqttServerDefinition.getMqttServerName());
				System.out.println("\tMQTT Client ID: " + mqttServerDefinition.getMqttClientId());
				System.out.println("\tMQTT Server URL: " + mqttServerDefinition.getMqttServerUrl());
				System.out.println("\tUsername: " + mqttServerDefinition.getUsername());
				System.out.println("\tPassword: ********");
				System.out.println("\tKeep Alive Timeout: " + mqttServerDefinition.getKeepAliveTimeout());
			}

			// Start the Host Application
			SparkplugHostApplication sparkplugHostApplication = new SparkplugHostApplication();
			sparkplugHostApplication.start();

			// Sleep a while
			Thread.sleep(360000);

			// Shutdown
			sparkplugHostApplication.shutdown();

		} catch (Exception e) {
			logger.error("Failed to run the Edge Node", e);
		}
	}

	public SparkplugHostApplication() {
		try {
			hostApplication = new HostApplication(this, HOST_ID,
					new ArrayList<>(Arrays.asList(SparkplugMeta.SPARKPLUG_B_TOPIC_PREFIX + "/#")),
					mqttServerDefinitions, null, new SparkplugBPayloadDecoder(), true);
		} catch (Exception e) {
			logger.error("Failed to create the HostApplication", e);
		}
	}

	public void start() throws TahuException {
		commandListener = new CommandListener(hostApplication, COMMAND_LISTENER_DIRECTORY, COMMAND_LISTENER_POLL_RATE);
		commandListener.start();
		hostApplication.start(true);
	}

	public void shutdown() {
		commandListener.shutdown();
		commandListener = null;
		hostApplication.shutdown();
	}

	@Override
	public void onConnect(MqttServerName serverName) {
		logger.info("onConnect...");
	}

	@Override
	public void onDisconnect(MqttServerName serverName) {
		logger.info("onDisconnect...");
	}

	@Override
	public void onNodeBirthArrived(MqttServerName serverName, EdgeNodeDescriptor edgeNodeDescriptor, Message message) {
		logger.info("onNodeBirthArrived from {}...", edgeNodeDescriptor);
	}

	@Override
	public void onNodeBirthComplete(MqttServerName serverName, EdgeNodeDescriptor edgeNodeDescriptor) {
		logger.info("onNodeBirthComplete from {}...", edgeNodeDescriptor);
	}

	@Override
	public void onNodeDataArrived(MqttServerName serverName, EdgeNodeDescriptor edgeNodeDescriptor, Message message) {
		logger.info("onNodeDataArrived from {}...", edgeNodeDescriptor);
	}

	@Override
	public void onNodeDataComplete(MqttServerName serverName, EdgeNodeDescriptor edgeNodeDescriptor) {
		logger.info("onNodeDataComplete from {}...", edgeNodeDescriptor);
	}

	@Override
	public void onNodeDeath(MqttServerName serverName, EdgeNodeDescriptor edgeNodeDescriptor, Message message) {
		logger.info("onNodeDeath from {}...", edgeNodeDescriptor);
	}

	@Override
	public void onNodeDeathComplete(MqttServerName serverName, EdgeNodeDescriptor edgeNodeDescriptor) {
		logger.info("onNodeDeathComplete from {}...", edgeNodeDescriptor);
	}

	@Override
	public void onDeviceBirthArrived(MqttServerName serverName, DeviceDescriptor deviceDescriptor, Message message) {
		logger.info("onDeviceBirthArrived from {}...", deviceDescriptor);
	}

	@Override
	public void onDeviceBirthComplete(MqttServerName serverName, DeviceDescriptor deviceDescriptor) {
		logger.info("onDeviceBirthComplete from {}...", deviceDescriptor);
	}

	@Override
	public void onDeviceDataArrived(MqttServerName serverName, DeviceDescriptor deviceDescriptor, Message message) {
		logger.info("onDeviceDataArrived from {}...", deviceDescriptor);
	}

	@Override
	public void onDeviceDataComplete(MqttServerName serverName, DeviceDescriptor deviceDescriptor) {
		logger.info("onDeviceDataComplete from {}...", deviceDescriptor);
	}

	@Override
	public void onDeviceDeath(MqttServerName serverName, DeviceDescriptor deviceDescriptor, Message message) {
		logger.info("onDeviceDeath from {}...", deviceDescriptor);
	}

	@Override
	public void onDeviceDeathComplete(MqttServerName serverName, DeviceDescriptor deviceDescriptor) {
		logger.info("onDeviceDeathComplete from {}...", deviceDescriptor);
	}

	@Override
	public void onBirthMetric(MqttServerName serverName, SparkplugDescriptor sparkplugDescriptor, Metric metric) {
		logger.info("onBirthMetric from {} with metric={}...", sparkplugDescriptor, metric);
	}

	@Override
	public void onDataMetric(MqttServerName serverName, SparkplugDescriptor sparkplugDescriptor, Metric metric) {
		logger.info("onDataMetric from {} with metric={}...", sparkplugDescriptor, metric);
	}

	public void onStale(MqttServerName serverName, SparkplugDescriptor sparkplugDescriptor, Metric metric) {
		logger.info("onStale from {} for {}...", sparkplugDescriptor, metric.getName());
	}

	@Override
	public void onMessage(MqttServerName serverName, SparkplugDescriptor sparkplugDescriptor, Message message) {
		logger.info("onMessage from {} with message={}...", sparkplugDescriptor, message);
	}
}
