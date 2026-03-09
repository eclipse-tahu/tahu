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
#print(sys.path)
import time

import paho.mqtt.client as mqtt
import sparkplug_b as sparkplug
import time
import random
import string

from sparkplug_b import *

# Application Variables
serverUrl = "localhost"
myGroupId = "Sparkplug B Devices"
myNodeName = "Python Edge Node 1"
myDeviceName = "Emulated Device"
publishPeriod = 5000
myUsername = "admin"
myPassword = "changeme"

#dDataValue = 0
#dDataTimestamp = int(round(time.time() * 1000))

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

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("spBv1.0/" + myGroupId + "/NCMD/" + myNodeName + "/#")
    client.subscribe("spBv1.0/" + myGroupId + "/DCMD/" + myNodeName + "/#")
######################################################################

######################################################################
# The callback for when a PUBLISH message is received from the server.
######################################################################
def on_message(client, userdata, msg):
    print("Message arrived: " + msg.topic)
    tokens = msg.topic.split("/")

    if tokens[0] == "spBv1.0" and tokens[1] == myGroupId and (tokens[2] == "NCMD" or tokens[2] == "DCMD") and tokens[3] == myNodeName:
        inboundPayload = sparkplug_b_pb2.Payload()
        inboundPayload.ParseFromString(msg.payload)
        for metric in inboundPayload.metrics:
            if metric.name == "Node Control/Next Server":
                # 'Node Control/Next Server' is an NCMD used to tell the device/client application to
                # disconnect from the current MQTT server and connect to the next MQTT server in the
                # list of available servers.  This is used for clients that have a pool of MQTT servers
                # to connect to.
                print( "'Node Control/Next Server' is not implemented in this example")
            elif metric.name == "Node Control/Rebirth":
                # 'Node Control/Rebirth' is an NCMD used to tell the device/client application to resend
                # its full NBIRTH and DBIRTH again.  MQTT Engine will send this NCMD to a device/client
                # application if it receives an NDATA or DDATA with a metric that was not published in the
                # original NBIRTH or DBIRTH.  This is why the application must send all known metrics in
                # its original NBIRTH and DBIRTH messages.
                publishBirth()
            elif metric.name == "Node Control/Reboot":
                # 'Node Control/Reboot' is an NCMD used to tell a device/client application to reboot
                # This can be used for devices that need a full application reset via a soft reboot.
                # In this case, we fake a full reboot with a republishing of the NBIRTH and DBIRTH
                # messages.
                publishBirth()
            elif metric.name == "output/Device Metric2":
                # This is a metric we declared in our DBIRTH message and we're emulating an output.
                # So, on incoming 'writes' to the output we must publish a DDATA with the new output
                # value.  If this were a real output we'd write to the output and then read it back
                # before publishing a DDATA message.

                # We know this is an Int16 because of how we declated it in the DBIRTH
                newValue = metric.int_value
                print( "CMD message for output/Device Metric2 - New Value: {}".format(newValue))

                # Create the DDATA payload
                payload = sparkplug.getDdataPayload()
                addMetric(payload, None, None, MetricDataType.Int16, newValue)

                # Publish a message data
                byteArray = bytearray(payload.SerializeToString())
                client.publish("spBv1.0/" + myGroupId + "/DDATA/" + myNodeName + "/" + myDeviceName, byteArray, 0, False)
            elif metric.name == "output/Device Metric3":
                # This is a metric we declared in our DBIRTH message and we're emulating an output.
                # So, on incoming 'writes' to the output we must publish a DDATA with the new output
                # value.  If this were a real output we'd write to the output and then read it back
                # before publishing a DDATA message.

                # We know this is an Boolean because of how we declated it in the DBIRTH
                newValue = metric.boolean_value
                print( "CMD message for output/Device Metric3 - New Value: %r" % newValue)

                # Create the DDATA payload
                payload = sparkplug.getDdataPayload()
                addMetric(payload, None, None, MetricDataType.Boolean, newValue)

                # Publish a message data
                byteArray = bytearray(payload.SerializeToString())
                client.publish("spBv1.0/" + myGroupId + "/DDATA/" + myNodeName + "/" + myDeviceName, byteArray, 0, False)
            else:
                print( "Unknown command: " + metric.name)
    else:
        print( "Unknown command...")

    print( "Done publishing")
######################################################################

######################################################################
# Publish the BIRTH certificates
######################################################################
def publishBirth():
    publishNodeBirth()
    publishDeviceBirth()
######################################################################

######################################################################
# Publish the NBIRTH certificate
######################################################################
def publishNodeBirth():
    print( "Publishing Node Birth")

    # Create the node birth payload
    payload = sparkplug.getNodeBirthPayload()

    # Set up the Node Controls
    addMetric(payload, "Node Control/Next Server", None, MetricDataType.Boolean, False)
    addMetric(payload, "Node Control/Rebirth", None, MetricDataType.Boolean, False)
    addMetric(payload, "Node Control/Reboot", None, MetricDataType.Boolean, False)

    # Add a regular node metric
    addMetric(payload, "Node Metric0", None, MetricDataType.String, "hello node")

    # Publish the node birth certificate
    byteArray = bytearray(payload.SerializeToString())
    client.publish("spBv1.0/" + myGroupId + "/NBIRTH/" + myNodeName, byteArray, 0, False)
######################################################################

######################################################################
# Publish the DBIRTH certificate
######################################################################
def publishDeviceBirth():
    print( "Publishing Device Birth")

    # Get the payload
    payload = sparkplug.getDeviceBirthPayload()

    # Add a device metric
    global dDataValue
    dDataValue = 0
    global dDataTimestamp
    dDataTimestamp = int(round(time.time() * 1000))
    addMetric(payload, "Device Metric", None, MetricDataType.Int32, dDataValue, dDataTimestamp)
    dDataValue += 1
    dDataTimestamp -= 1000

    # Publish the initial data with the Device BIRTH certificate
    totalByteArray = bytearray(payload.SerializeToString())
    client.publish("spBv1.0/" + myGroupId + "/DBIRTH/" + myNodeName + "/" + myDeviceName, totalByteArray, 0, False)
######################################################################

######################################################################
# Main Application
######################################################################
print("Starting main application")

# Create the node death payload
deathPayload = sparkplug.getNodeDeathPayload()

# Start of main program - Set up the MQTT client connection
client = mqtt.Client(serverUrl, 1883, 60)
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(myUsername, myPassword)
deathByteArray = bytearray(deathPayload.SerializeToString())
client.will_set("spBv1.0/" + myGroupId + "/NDEATH/" + myNodeName, deathByteArray, 0, False)
client.connect(serverUrl, 1883, 60)

# Short delay to allow connect callback to occur
time.sleep(.1)
client.loop()

# Publish the birth certificates
publishBirth()

while True:
    # Periodically publish some new data
    payload = sparkplug.getDdataPayload()

    # Add some random data to the inputs
    addMetric(payload, "Device Metric", None, MetricDataType.Int32, dDataValue, dDataTimestamp)
    dDataValue += 1
    dDataTimestamp -= 1000

    # Publish a message data
    byteArray = bytearray(payload.SerializeToString())
    client.publish("spBv1.0/" + myGroupId + "/DDATA/" + myNodeName + "/" + myDeviceName, byteArray, 0, False)

    # Sit and wait for inbound or outbound events
    for _ in range(5):
        time.sleep(.1)
        client.loop()
######################################################################
