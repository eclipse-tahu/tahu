/********************************************************************************
 * Copyright (c) 2025 Cirrus Link Solutions and others
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

package org.eclipse.tahu.host.api;

import org.eclipse.tahu.message.model.DeviceDescriptor;
import org.eclipse.tahu.message.model.EdgeNodeDescriptor;
import org.eclipse.tahu.message.model.Message;
import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.SparkplugDescriptor;
import org.eclipse.tahu.mqtt.MqttServerName;

public interface MultiHostApplicationEventHandler {

	public void onConnect(MqttServerName serverName);

	public void onDisconnect(MqttServerName serverName);

	public void onMessage(MqttServerName serverName, SparkplugDescriptor sparkplugDescriptor, Message message);

	public void onNodeBirthArrived(MqttServerName serverName, EdgeNodeDescriptor edgeNodeDescriptor, Message message);

	public void onNodeBirthComplete(MqttServerName serverName, EdgeNodeDescriptor edgeNodeDescriptor);

	public void onNodeDataArrived(MqttServerName serverName, EdgeNodeDescriptor edgeNodeDescriptor, Message message);

	public void onNodeDataComplete(MqttServerName serverName, EdgeNodeDescriptor edgeNodeDescriptor);

	public void onNodeDeath(MqttServerName serverName, EdgeNodeDescriptor edgeNodeDescriptor, Message message);

	public void onNodeDeathComplete(MqttServerName serverName, EdgeNodeDescriptor edgeNodeDescriptor);

	public void onDeviceBirthArrived(MqttServerName serverName, DeviceDescriptor deviceDescriptor, Message message);

	public void onDeviceBirthComplete(MqttServerName serverName, DeviceDescriptor deviceDescriptor);

	public void onDeviceDataArrived(MqttServerName serverName, DeviceDescriptor deviceDescriptor, Message message);

	public void onDeviceDataComplete(MqttServerName serverName, DeviceDescriptor deviceDescriptor);

	public void onDeviceDeath(MqttServerName serverName, DeviceDescriptor deviceDescriptor, Message message);

	public void onDeviceDeathComplete(MqttServerName serverName, DeviceDescriptor deviceDescriptor);

	public void onBirthMetric(MqttServerName serverName, SparkplugDescriptor sparkplugDescriptor, Metric metric);

	public void onDataMetric(MqttServerName serverName, SparkplugDescriptor sparkplugDescriptor, Metric metric);

	public void onStale(MqttServerName serverName, SparkplugDescriptor sparkplugDescriptor, Metric metric);
}
