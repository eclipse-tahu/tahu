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
publishPeriod = 5000
myUsername = "admin"
myPassword = "changeme"

class AliasMap:
    Next_Server = 0
    Rebirth = 1
    Reboot = 2
    Dataset = 3
    Node_Metric0 = 4
    Node_Metric1 = 5
    Node_Metric2 = 6
    Node_Metric3 = 7
    Node_Metric4 = 8

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
    global my0NodeName

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("spBv1.0/" + myGroupId + "/NCMD/" + myNodeName + "/#")
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
            if metric.name == "Node Control/Next Server" or metric.alias == AliasMap.Next_Server:
                # 'Node Control/Next Server' is an NCMD used to tell the device/client application to
                # disconnect from the current MQTT server and connect to the next MQTT server in the
                # list of available servers.  This is used for clients that have a pool of MQTT servers
                # to connect to.
                print( "'Node Control/Next Server' is not implemented in this example")
            elif metric.name == "Node Control/Rebirth" or metric.alias == AliasMap.Rebirth:
                # 'Node Control/Rebirth' is an NCMD used to tell the device/client application to resend
                # its full NBIRTH and DBIRTH again.  MQTT Engine will send this NCMD to a device/client
                # application if it receives an NDATA or DDATA with a metric that was not published in the
                # original NBIRTH or DBIRTH.  This is why the application must send all known metrics in
                # its original NBIRTH and DBIRTH messages.
                publishBirth()
            elif metric.name == "Node Control/Reboot" or metric.alias == AliasMap.Reboot:
                # 'Node Control/Reboot' is an NCMD used to tell a device/client application to reboot
                # This can be used for devices that need a full application reset via a soft reboot.
                # In this case, we fake a full reboot with a republishing of the NBIRTH and DBIRTH
                # messages.
                publishBirth()
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
######################################################################

######################################################################
# Publish the NBIRTH certificate
######################################################################
def publishNodeBirth():
    print( "Publishing Node Birth")

    # Create the node birth payload
    payload = sparkplug.getNodeBirthPayload()

    # Set up the Node Controls
    addMetric(payload, "Node Control/Next Server", AliasMap.Next_Server, MetricDataType.Boolean, False)
    addMetric(payload, "Node Control/Rebirth", AliasMap.Rebirth, MetricDataType.Boolean, False)
    addMetric(payload, "Node Control/Reboot", AliasMap.Reboot, MetricDataType.Boolean, False)

    # Add some regular node metrics
    addMetric(payload, "Node Metric0", AliasMap.Node_Metric0, MetricDataType.String, "hello node")
    addMetric(payload, "Node Metric1", AliasMap.Node_Metric1, MetricDataType.Boolean, True)
    addNullMetric(payload, "Node Metric3", AliasMap.Node_Metric3, MetricDataType.Int32)
    addMetric(payload, "Node Control/Current Second", AliasMap.Node_Metric4, MetricDataType.Int32, 11)

    # Create a DataSet (012 - 345) two rows with Int8, Int16, and Int32 contents and headers Int8s, Int16s, Int32s and add it to the payload
    columns = ["Int8s", "Int16s", "Int32s"]
    types = [DataSetDataType.Int8, DataSetDataType.Int16, DataSetDataType.Int32]
    dataset = initDatasetMetric(payload, "DataSet", AliasMap.Dataset, columns, types)
    row = dataset.rows.add()
    element = row.elements.add();
    element.int_value = 0
    element = row.elements.add();
    element.int_value = 1
    element = row.elements.add();
    element.int_value = 2
    row = dataset.rows.add()
    element = row.elements.add();
    element.int_value = 3
    element = row.elements.add();
    element.int_value = 4
    element = row.elements.add();
    element.int_value = 5

    # Add a metric with a custom property
    metric = addMetric(payload, "Node Metric2", AliasMap.Node_Metric2, MetricDataType.Int16, 13)
    metric.properties.keys.extend(["engUnit"])
    propertyValue = metric.properties.values.add()
    propertyValue.type = ParameterDataType.String
    propertyValue.string_value = "MyCustomUnits"

    # Create the UDT definition value which includes two UDT members and a single parameter and add it to the payload
    template = initTemplateMetric(payload, "_types_/Custom_Motor", None, None)    # No alias for Template definitions
    templateParameter = template.parameters.add()
    templateParameter.name = "Index"
    templateParameter.type = ParameterDataType.String
    templateParameter.string_value = "0"
    addMetric(template, "RPMs", None, MetricDataType.Int32, 0)    # No alias in UDT members
    addMetric(template, "AMPs", None, MetricDataType.Int32, 0)    # No alias in UDT members

    # Publish the node birth certificate
    byteArray = bytearray(payload.SerializeToString())
    client.publish("spBv1.0/" + myGroupId + "/NBIRTH/" + myNodeName, byteArray, 0, False)
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
    # NDATA
    nodeDataPayload = sparkplug.getDdataPayload()
    addMetric(nodeDataPayload, None, AliasMap.Node_Metric3, MetricDataType.Int32, random.randrange(0, 100))
    addMetric(nodeDataPayload, None, AliasMap.Node_Metric4, MetricDataType.Int32, random.randrange(0, 100))
    byteArray = bytearray(nodeDataPayload.SerializeToString())
    client.publish("spBv1.0/" + myGroupId + "/NDATA/" + myNodeName, byteArray, 0, False)

    # Sit and wait for inbound or outbound events
    for _ in range(5):
        time.sleep(.1)
        client.loop()
######################################################################
