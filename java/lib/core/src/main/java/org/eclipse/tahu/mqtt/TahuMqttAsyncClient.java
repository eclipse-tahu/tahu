/*
 * Licensed Materials - Property of Cirrus Link Solutions
 * Copyright (c) 2018 Cirrus Link Solutions LLC - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 */
package org.eclipse.tahu.mqtt;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;

import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttPingSender;
import org.eclipse.paho.client.mqttv3.internal.HighResolutionTimer;

public class TahuMqttAsyncClient extends MqttAsyncClient {

	private final Date clientInitDateTime = new Date();

	public TahuMqttAsyncClient(String serverURI, String clientId, MqttClientPersistence persistence,
			MqttPingSender pingSender, ScheduledExecutorService executorService,
			HighResolutionTimer highResolutionTimer) throws MqttException {
		super(serverURI, clientId, persistence, pingSender, executorService, highResolutionTimer);
	}

	public TahuMqttAsyncClient(String serverURI, String clientId, MqttClientPersistence persistence,
			MqttPingSender pingSender, ScheduledExecutorService executorService) throws MqttException {
		super(serverURI, clientId, persistence, pingSender, executorService);
	}

	public TahuMqttAsyncClient(String serverURI, String clientId, MqttClientPersistence persistence,
			MqttPingSender pingSender) throws MqttException {
		super(serverURI, clientId, persistence, pingSender);
	}

	public TahuMqttAsyncClient(String serverURI, String clientId, MqttClientPersistence persistence)
			throws MqttException {
		super(serverURI, clientId, persistence);
	}

	public TahuMqttAsyncClient(String serverURI, String clientId) throws MqttException {
		super(serverURI, clientId);
	}

	public Date getClientInitDateTime() {
		return clientInitDateTime;
	}

	public Properties getClientCommsDebug() {
		if (comms != null) {
			return comms.getDebug();
		} else {
			return null;
		}
	}

	public Properties getClientStateDebug() {
		if (comms != null && comms.getClientState() != null) {
			return comms.getClientState().getDebug();
		} else {
			return null;
		}
	}

	public Properties getConOptions() {
		if (comms != null) {
			return comms.getConOptions().getDebug();
		} else {
			return null;
		}
	}
}
