#!/usr/bin/python
#/********************************************************************************
# * Copyright (c) 2014, 2018 Cirrus Link Solutions and others
# *
# * This program and the accompanying materials are made available under the
# * terms of the Eclipse Public License 2.0 which is available at
# * http://www.eclipse.org/legal/epl-2.0.
# *
# * SPDX-License-Identifier: EPL-2.0
# *
# * Contributors:
# *   Cirrus Link Solutions - initial implementation
# ********************************************************************************/
import sys
sys.path.insert(0, "../core/")
print(sys.path)

import paho.mqtt.client as mqtt
import sparkplug_b as sparkplug
import time
import random
import string

from sparkplug_b import *

# Application Variables
serverUrl = "localhost"
myGroupId = "G1"
myNodeName = "E1"
myDeviceName = "D1"

publishPeriod = 5000
myUsername = "admin"
myPassword = "changeme"

######################################################################
# The callback for when the client receives a CONNACK response from the server.
######################################################################
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected with result code "+str(rc))
    else:
        print("Failed to connect with result code "+str(rc))
        sys.exit()

    global myGroupId
    global myNodeName
######################################################################

######################################################################
# The callback for when a PUBLISH message is received from the server.
######################################################################
def on_message(client, userdata, msg):
    print("Message arrived: " + msg.topic)
######################################################################

#######################################################################
# Publish NCMD message with a single aliased metric
#######################################################################
def publishNodeCommandWithAliasedMetric(value):
    print("publishing Command Message with aliased Metric")
    payload = sparkplug.getCmdPayload()

    addMetric(payload, None, 0, MetricDataType.Int32, value)
    totalByteArray = bytearray(payload.SerializeToString())

    client.publish("spBv1.0/" + myGroupId + "/NCMD/" + myNodeName, totalByteArray, 0, False)
    print(payload)

    print(type(payload))
######################################################################

#######################################################################
# Publish NCMD message with a bunch of metrics
#######################################################################
def publishNodeCommandWithMetrics(numMetrics, value):
    print("publishing Command Message with Metrics")
    payload = sparkplug.getCmdPayload()

    for i in range(numMetrics):
        addMetric(payload, "Example Tag "+str(i), None, MetricDataType.Int32, value)
    totalByteArray = bytearray(payload.SerializeToString())

    client.publish("spBv1.0/" + myGroupId + "/NCMD/" + myNodeName, totalByteArray, 0, False)
    print(payload)

    print(type(payload))
######################################################################

#######################################################################
# Publish DCMD message with a bunch of metrics
#######################################################################
def publishDeviceCommandWithMetrics(numMetrics, value):
    print("publishing Command Message with Metrics")
    payload = sparkplug.getCmdPayload()

    for i in range(numMetrics):
        addMetric(payload, "T" + str(i), None, MetricDataType.Int32, value)
    totalByteArray = bytearray(payload.SerializeToString())

    client.publish("spBv1.0/" + myGroupId + "/DCMD/" + myNodeName + "/" + myDeviceName, totalByteArray, 0, False)
    print(payload)

    print(type(payload))
#######################################################################

######################################################################
# Main Application
######################################################################
print("Starting main application")

# Start of main program - Set up the MQTT client connection
#client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, serverUrl, 1883, 60)
client = mqtt.Client(serverUrl, 1883, 60)
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(myUsername, myPassword)

client.connect(serverUrl, 1883, 60)

# Short delay to allow connect callback to occur
time.sleep(.1)
client.loop()

#publishNodeCommandWithMetrics(1500, 100)
#publishDeviceCommandWithMetrics(5, 115)
publishNodeCommandWithAliasedMetric(10)
######################################################################
