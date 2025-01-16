/********************************************************************************
 * Copyright (c) 2014-2022 Cirrus Link Solutions and others
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

package org.eclipse.tahu;

import static org.eclipse.tahu.message.model.MetricDataType.Boolean;
import static org.eclipse.tahu.message.model.MetricDataType.DataSet;
import static org.eclipse.tahu.message.model.MetricDataType.DateTime;
import static org.eclipse.tahu.message.model.MetricDataType.Double;
import static org.eclipse.tahu.message.model.MetricDataType.Float;
import static org.eclipse.tahu.message.model.MetricDataType.Int16;
import static org.eclipse.tahu.message.model.MetricDataType.Int32;
import static org.eclipse.tahu.message.model.MetricDataType.Int64;
import static org.eclipse.tahu.message.model.MetricDataType.Int8;
import static org.eclipse.tahu.message.model.MetricDataType.String;
import static org.eclipse.tahu.message.model.MetricDataType.Template;
import static org.eclipse.tahu.message.model.MetricDataType.Text;
import static org.eclipse.tahu.message.model.MetricDataType.UInt16;
import static org.eclipse.tahu.message.model.MetricDataType.UInt32;
import static org.eclipse.tahu.message.model.MetricDataType.UInt64;
import static org.eclipse.tahu.message.model.MetricDataType.UInt8;
import static org.eclipse.tahu.message.model.MetricDataType.UUID;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.tahu.message.SparkplugBPayloadDecoder;
import org.eclipse.tahu.message.SparkplugBPayloadEncoder;
import org.eclipse.tahu.message.model.DataSet;
import org.eclipse.tahu.message.model.DataSet.DataSetBuilder;
import org.eclipse.tahu.message.model.DataSetDataType;
import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.Metric.MetricBuilder;
import org.eclipse.tahu.message.model.Parameter;
import org.eclipse.tahu.message.model.ParameterDataType;
import org.eclipse.tahu.message.model.PropertyDataType;
import org.eclipse.tahu.message.model.PropertySet;
import org.eclipse.tahu.message.model.PropertySet.PropertySetBuilder;
import org.eclipse.tahu.message.model.PropertyValue;
import org.eclipse.tahu.message.model.Row.RowBuilder;
import org.eclipse.tahu.message.model.SparkplugBPayload;
import org.eclipse.tahu.message.model.SparkplugBPayload.SparkplugBPayloadBuilder;
import org.eclipse.tahu.message.model.Template;
import org.eclipse.tahu.message.model.Template.TemplateBuilder;
import org.eclipse.tahu.message.model.Value;
import org.eclipse.tahu.util.CompressionAlgorithm;
import org.eclipse.tahu.util.PayloadUtil;

/**
 * An example Sparkplug B application.
 */
public class SparkplugExample implements MqttCallbackExtended {

	// HW/SW versions
	private static final String HW_VERSION = "Emulated Hardware";
	private static final String SW_VERSION = "v1.0.0";

	private static final String NAMESPACE = "spBv1.0";

	// Configuration
	private static final boolean USING_REAL_TLS = false;
	private static final boolean USING_COMPRESSION = false;
	private static final CompressionAlgorithm compressionAlgorithm = CompressionAlgorithm.GZIP;
	private String serverUrl = "tcp://localhost:1883";
	private String groupId = "Sparkplug B Devices";
	private String edgeNode = "Java Sparkplug B Example";
	private String deviceId = "SparkplugBExample";
	private String clientId = "SparkplugBExampleEdgeNode";
	private String username = "admin";
	private String password = "changeme";
	private long PUBLISH_PERIOD = 60000; // Publish period in milliseconds
	private ExecutorService executor;
	private MqttClient client;

	private int bdSeq = 0;
	private int seq = 0;

	private Object seqLock = new Object();

	public static void main(String[] args) {
		SparkplugExample example = new SparkplugExample();
		example.run();
	}

	public void run() {
		try {
			// Random generator and thread pool for outgoing published messages
			executor = Executors.newFixedThreadPool(1);

			// Build up DEATH payload - note DEATH payloads don't have a regular sequence number
			SparkplugBPayloadBuilder deathPayload = new SparkplugBPayloadBuilder().setTimestamp(new Date());
			deathPayload = addBdSeqNum(deathPayload);
			byte[] deathBytes;
			if (USING_COMPRESSION) {
				// Compress payload (optional)
				deathBytes = new SparkplugBPayloadEncoder().getBytes(
						PayloadUtil.compress(deathPayload.createPayload(), compressionAlgorithm, false), false);
			} else {
				deathBytes = new SparkplugBPayloadEncoder().getBytes(deathPayload.createPayload(), false);
			}

			MqttConnectOptions options = new MqttConnectOptions();

			if (USING_REAL_TLS) {
				SocketFactory sf = SSLSocketFactory.getDefault();
				options.setSocketFactory(sf);
			}

			// Connect to the MQTT Server
			options.setAutomaticReconnect(true);
			options.setCleanSession(true);
			options.setConnectionTimeout(30);
			options.setKeepAliveInterval(30);
			options.setUserName(username);
			options.setPassword(password.toCharArray());
			options.setWill(NAMESPACE + "/" + groupId + "/NDEATH/" + edgeNode, deathBytes, 0, false);
			client = new MqttClient(serverUrl, clientId);
			client.setTimeToWait(2000);
			client.setCallback(this); // short timeout on failure to connect
			client.connect(options);

			// Subscribe to control/command messages for both the edge of network node and the attached devices
			client.subscribe(NAMESPACE + "/" + groupId + "/NCMD/" + edgeNode + "/#", 0);
			client.subscribe(NAMESPACE + "/" + groupId + "/DCMD/" + edgeNode + "/#", 0);
			client.subscribe(NAMESPACE + "/#", 0);

			// Loop forever publishing data every PUBLISH_PERIOD
			while (true) {
				Thread.sleep(PUBLISH_PERIOD);

				if (client.isConnected()) {
					synchronized (seqLock) {
						System.out.println("Connected - publishing new data");
						// Create the payload and add some metrics
						SparkplugBPayload payload =
								new SparkplugBPayload(new Date(), newMetrics(false), getSeqNum(), newUUID(), null);

						// Compress payload (optional)
						if (USING_COMPRESSION) {
							client.publish(NAMESPACE + "/" + groupId + "/DDATA/" + edgeNode + "/" + deviceId,
									new SparkplugBPayloadEncoder().getBytes(
											PayloadUtil.compress(payload, compressionAlgorithm, false), false),
									0, false);
						} else {
							client.publish(NAMESPACE + "/" + groupId + "/DDATA/" + edgeNode + "/" + deviceId,
									new SparkplugBPayloadEncoder().getBytes(payload, false), 0, false);
						}
					}
				} else {
					System.out.println("Not connected - not publishing data");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private byte[] randomBytes(int numOfBytes) {
		byte[] bytes = new byte[numOfBytes];
		new Random().nextBytes(bytes);
		return bytes;
	}

	private void publishBirth() {
		publishNodeBirth();
		publishDeviceBirth();
	}

	private void publishNodeBirth() {
		try {
			synchronized (seqLock) {
				// Reset the sequence number
				seq = 0;

				// Create the BIRTH payload and set the position and other metrics
				SparkplugBPayload payload =
						new SparkplugBPayload(new Date(), new ArrayList<Metric>(), getSeqNum(), newUUID(), null);

				payload.addMetric(new MetricBuilder("bdSeq", Int64, (long) bdSeq).createMetric());
				payload.addMetric(new MetricBuilder("Node Control/Rebirth", Boolean, false).createMetric());

				PropertySet nestedPropertySet = new PropertySetBuilder()
						.addProperty("custom", new PropertyValue<>(PropertyDataType.String, "Custom Value"))
						.createPropertySet();

				PropertySet propertySet = new PropertySetBuilder()
						.addProperty("engUnit", new PropertyValue<>(PropertyDataType.String, "My Units"))
						.addProperty("engLow", new PropertyValue<>(PropertyDataType.Double, 1.0))
						.addProperty("engHigh", new PropertyValue<>(PropertyDataType.Double, 10.0))
						.addProperty("Custom nested node prop",
								new PropertyValue<>(PropertyDataType.PropertySet, nestedPropertySet))
						/*
						 * .addProperty("CustA", new PropertyValue(PropertyDataType.String, "Custom A"))
						 * .addProperty("CustB", new PropertyValue(PropertyDataType.Double, 10.0)) .addProperty("CustC",
						 * new PropertyValue(PropertyDataType.Int32, 100))
						 */
						.createPropertySet();
				payload.addMetric(
						new MetricBuilder("MyMetric", String, "My Value").properties(propertySet).createMetric());

				System.out.println("Publishing Edge Node Birth");
				executor.execute(new Publisher(NAMESPACE + "/" + groupId + "/NBIRTH/" + edgeNode, payload));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void publishDeviceBirth() {
		try {
			synchronized (seqLock) {
				// Create the payload and add some metrics
				SparkplugBPayload payload =
						new SparkplugBPayload(new Date(), newMetrics(true), getSeqNum(), newUUID(), null);

				payload.addMetric(new MetricBuilder("Device Control/Rebirth", Boolean, false).createMetric());

				payload.addMetric(new MetricBuilder("Int8_Min", Int8, (byte) -128).createMetric());
				payload.addMetric(new MetricBuilder("Int8_Max", Int8, (byte) 127).createMetric());
				payload.addMetric(new MetricBuilder("Int16_Min", Int16, (short) -32768).createMetric());
				payload.addMetric(new MetricBuilder("Int16_Max", Int16, (short) 32767).createMetric());
				payload.addMetric(new MetricBuilder("Int32_Min", Int32, -2147483648).createMetric());
				payload.addMetric(new MetricBuilder("Int32_Max", Int32, 2147483647).createMetric());
				payload.addMetric(new MetricBuilder("Int64_Min", Int64, -9223372036854775808L).createMetric());
				payload.addMetric(new MetricBuilder("Int64_Max", Int64, 9223372036854775807L).createMetric());
				payload.addMetric(new MetricBuilder("UInt8_Min", UInt8, (short) 0).createMetric());
				payload.addMetric(new MetricBuilder("UInt8_Max", UInt8, (short) 255).createMetric());
				payload.addMetric(new MetricBuilder("UInt16_Min", UInt16, 0).createMetric());
				payload.addMetric(new MetricBuilder("UInt16_Max", UInt16, 64535).createMetric());
				payload.addMetric(new MetricBuilder("UInt32_Min", UInt32, 0L).createMetric());
				payload.addMetric(new MetricBuilder("UInt32_Max", UInt32, 4294967295L).createMetric());
				payload.addMetric(new MetricBuilder("UInt64_Min", UInt64, BigInteger.valueOf(0L)).createMetric());
				payload.addMetric(
						new MetricBuilder("UInt64_Max", UInt64, new BigInteger("18446744073709551615")).createMetric());

				// Add some properties
				payload.addMetric(new MetricBuilder("Properties/hw_version", String, HW_VERSION).createMetric());
				payload.addMetric(new MetricBuilder("Properties/sw_version", String, SW_VERSION).createMetric());

				PropertySet nestedPropertySet = new PropertySetBuilder()
						.addProperty("custom", new PropertyValue<>(PropertyDataType.String, "Custom Value"))
						.createPropertySet();

				PropertySet propertySet = new PropertySetBuilder()
						.addProperty("engUnit", new PropertyValue<>(PropertyDataType.String, "My Units"))
						.addProperty("engLow", new PropertyValue<>(PropertyDataType.Double, 1.0))
						.addProperty("engHigh", new PropertyValue<>(PropertyDataType.Double, 10.0))
						.addProperty("Custom nested device prop",
								new PropertyValue<>(PropertyDataType.PropertySet, nestedPropertySet))
						/*
						 * .addProperty("CustA", new PropertyValue(PropertyDataType.String, "Custom A"))
						 * .addProperty("CustB", new PropertyValue(PropertyDataType.Double, 10.0)) .addProperty("CustC",
						 * new PropertyValue(PropertyDataType.Int32, 100))
						 */
						.createPropertySet();
				payload.addMetric(
						new MetricBuilder("MyMetric", String, "My Value").properties(propertySet).createMetric());

				System.out.println("Publishing Device Birth");
				executor.execute(
						new Publisher(NAMESPACE + "/" + groupId + "/DBIRTH/" + edgeNode + "/" + deviceId, payload));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// Used to add the birth/death sequence number
	private SparkplugBPayloadBuilder addBdSeqNum(SparkplugBPayloadBuilder payload) throws Exception {
		if (payload == null) {
			payload = new SparkplugBPayloadBuilder();
		}
		if (bdSeq == 256) {
			bdSeq = 0;
		}
		payload.addMetric(new MetricBuilder("bdSeq", Int64, (long) bdSeq).createMetric());
		bdSeq++;
		return payload;
	}

	// Used to add the sequence number
	private long getSeqNum() throws Exception {
		System.out.println("seq: " + seq);
		if (seq == 256) {
			seq = 0;
		}
		return seq++;
	}

	@Override
	public void connectComplete(boolean reconnect, String serverURI) {
		System.out.println("Connected! - publishing birth");
		publishBirth();
	}

	public void connectionLost(Throwable cause) {
		cause.printStackTrace();
		System.out.println("The MQTT Connection was lost! - will auto-reconnect");
	}

	public void messageArrived(String topic, MqttMessage message) throws Exception {
		System.out.println("Message Arrived on topic " + topic);

		SparkplugBPayloadDecoder decoder = new SparkplugBPayloadDecoder();
		SparkplugBPayload inboundPayload = decoder.buildFromByteArray(message.getPayload(), null);

		// Debug
		for (Metric metric : inboundPayload.getMetrics()) {
			System.out.println("Metric " + metric.getName() + "=" + metric.getValue());
		}

		String[] splitTopic = topic.split("/");
		if (splitTopic[0].equals(NAMESPACE) && splitTopic[1].equals(groupId) && splitTopic[2].equals("NCMD")
				&& splitTopic[3].equals(edgeNode)) {
			for (Metric metric : inboundPayload.getMetrics()) {
				if ("Node Control/Rebirth".equals(metric.getName()) && ((Boolean) metric.getValue())) {
					publishBirth();
				} else {
					System.out.println("Unknown Node Command NCMD: " + metric.getName());
				}
			}
		} else if (splitTopic[0].equals(NAMESPACE) && splitTopic[1].equals(groupId) && splitTopic[2].equals("DCMD")
				&& splitTopic[3].equals(edgeNode)) {
			System.out.println("Command recevied for device " + splitTopic[4]);

			// Process the incoming payload and publish any updated/modified metrics
			// Simulate the following:
			// Outputs/0 is tied to Inputs/0
			// Outputs/2 is tied to Inputs/1
			// Outputs/2 is tied to Inputs/2
			SparkplugBPayload outboundPayload =
					new SparkplugBPayload(new Date(), new ArrayList<Metric>(), getSeqNum(), newUUID(), null);
			for (Metric metric : inboundPayload.getMetrics()) {
				String name = metric.getName();
				Object value = metric.getValue();
				if ("Device Control/Rebirth".equals(metric.getName()) && ((Boolean) metric.getValue())) {
					publishDeviceBirth();
				} else if ("Outputs/0".equals(name)) {
					System.out.println("Outputs/0: " + value);
					outboundPayload.addMetric(new MetricBuilder("Inputs/0", Boolean, value).createMetric());
					outboundPayload.addMetric(new MetricBuilder("Outputs/0", Boolean, value).createMetric());
					System.out.println("Publishing updated value for Inputs/0 " + value);
				} else if ("Outputs/1".equals(name)) {
					System.out.println("Output1: " + value);
					outboundPayload.addMetric(new MetricBuilder("Inputs/1", Int32, value).createMetric());
					outboundPayload.addMetric(new MetricBuilder("Outputs/1", Int32, value).createMetric());
					System.out.println("Publishing updated value for Inputs/1 " + value);
				} else if ("Outputs/2".equals(name)) {
					System.out.println("Output2: " + value);
					outboundPayload.addMetric(new MetricBuilder("Inputs/2", Double, value).createMetric());
					outboundPayload.addMetric(new MetricBuilder("Outputs/2", Double, value).createMetric());
					System.out.println("Publishing updated value for Inputs/2 " + value);
				}
			}

			// Publish the message in a new thread
			executor.execute(
					new Publisher(NAMESPACE + "/" + groupId + "/DDATA/" + edgeNode + "/" + deviceId, outboundPayload));
		}
	}

	public void deliveryComplete(IMqttDeliveryToken token) {
		System.out.println("Published message: " + token);
	}

	private String newUUID() {
		return java.util.UUID.randomUUID().toString();
	}

	private List<Metric> newMetrics(boolean isBirth) throws SparkplugException {
		Random random = new Random();
		List<Metric> metrics = new ArrayList<Metric>();
		metrics.add(new MetricBuilder("Int8", Int8, (byte) random.nextInt()).createMetric());
		metrics.add(new MetricBuilder("Int16", Int16, (short) random.nextInt()).createMetric());
		metrics.add(new MetricBuilder("Int32", Int32, random.nextInt()).createMetric());
		metrics.add(new MetricBuilder("Int64", Int64, random.nextLong()).createMetric());
		metrics.add(new MetricBuilder("UInt8", UInt8, getRandomUInt8()).createMetric());
		metrics.add(new MetricBuilder("UInt16", UInt16, getRandomUInt16()).createMetric());
		metrics.add(new MetricBuilder("UInt32", UInt32, getRandomUInt32()).createMetric());
		metrics.add(new MetricBuilder("UInt64", UInt64, getRandomUInt64()).createMetric());
		metrics.add(new MetricBuilder("Float", Float, random.nextFloat()).createMetric());
		metrics.add(new MetricBuilder("Double", Double, random.nextDouble()).createMetric());
		metrics.add(new MetricBuilder("Boolean", Boolean, random.nextBoolean()).createMetric());
		metrics.add(new MetricBuilder("String", String, newUUID()).createMetric());
		metrics.add(new MetricBuilder("DateTime", DateTime, new Date()).createMetric());
		metrics.add(new MetricBuilder("Text", Text, newUUID()).createMetric());
		metrics.add(new MetricBuilder("UUID", UUID, newUUID()).createMetric());
		// metrics.add(new MetricBuilder("Bytes", Bytes, randomBytes(20)).createMetric());
		// metrics.add(new MetricBuilder("File", File, null).createMetric());

		// DataSet
		metrics.add(new MetricBuilder("DataSet", DataSet, newDataSet()).createMetric());
		if (isBirth) {
			metrics.add(new MetricBuilder("TemplateDef", Template, newTemplate(true, null)).createMetric());
		}

		// Template
		metrics.add(new MetricBuilder("TemplateInst", Template, newTemplate(false, "TemplateDef")).createMetric());

		// Complex Template
		metrics.addAll(newComplexTemplate(isBirth));

		// Metrics with properties
		metrics.add(new MetricBuilder("IntWithProps", Int32, random.nextInt()).properties(new PropertySetBuilder()
				.addProperty("engUnit", new PropertyValue<>(PropertyDataType.String, "My Units"))
				.addProperty("engHigh", new PropertyValue<>(PropertyDataType.Int32, Integer.MAX_VALUE))
				.addProperty("engLow", new PropertyValue<>(PropertyDataType.Int32, Integer.MIN_VALUE))
				.createPropertySet()).createMetric());

		// Aliased metric
		// The name and alias will be specified in a NBIRTH/DBIRTH message.
		// Only the alias will be specified in a NDATA/DDATA message.
		Long alias = 1111L;
		if (isBirth) {
			metrics.add(new MetricBuilder("AliasedString", String, newUUID()).alias(alias).createMetric());
		} else {
			metrics.add(new MetricBuilder(alias, String, newUUID()).createMetric());
		}

		return metrics;
	}

	private PropertySet newPropertySet() throws SparkplugException {
		return new PropertySetBuilder().addProperties(newProps(true)).createPropertySet();
	}

	private Map<String, PropertyValue<? extends PropertyDataType>> newProps(boolean withPropTypes)
			throws SparkplugException {
		Random random = new Random();
		Map<String, PropertyValue<? extends PropertyDataType>> propMap = new HashMap<>();
		propMap.put("PropInt8", new PropertyValue<>(PropertyDataType.Int8, (byte) random.nextInt()));
		propMap.put("PropInt16", new PropertyValue<>(PropertyDataType.Int16, (short) random.nextInt()));
		propMap.put("PropInt32", new PropertyValue<>(PropertyDataType.Int32, random.nextInt()));
		propMap.put("PropInt64", new PropertyValue<>(PropertyDataType.Int64, random.nextLong()));
		propMap.put("PropUInt8", new PropertyValue<>(PropertyDataType.UInt8, getRandomUInt8()));
		propMap.put("PropUInt16", new PropertyValue<>(PropertyDataType.UInt16, getRandomUInt16()));
		propMap.put("PropUInt32", new PropertyValue<>(PropertyDataType.UInt32, getRandomUInt32()));
		propMap.put("PropUInt64", new PropertyValue<>(PropertyDataType.UInt64, getRandomUInt64()));
		propMap.put("PropFloat", new PropertyValue<>(PropertyDataType.Float, random.nextFloat()));
		propMap.put("PropDouble", new PropertyValue<>(PropertyDataType.Double, random.nextDouble()));
		propMap.put("PropBoolean", new PropertyValue<>(PropertyDataType.Boolean, random.nextBoolean()));
		propMap.put("PropString", new PropertyValue<>(PropertyDataType.String, newUUID()));
		propMap.put("PropDateTime", new PropertyValue<>(PropertyDataType.DateTime, new Date()));
		propMap.put("PropText", new PropertyValue<>(PropertyDataType.Text, newUUID()));
		if (withPropTypes) {
			propMap.put("PropPropertySet", new PropertyValue<>(PropertyDataType.PropertySet,
					new PropertySetBuilder().addProperties(newProps(false)).createPropertySet()));
			List<PropertySet> propsList = new ArrayList<PropertySet>();
			propsList.add(new PropertySetBuilder().addProperties(newProps(false)).createPropertySet());
			propsList.add(new PropertySetBuilder().addProperties(newProps(false)).createPropertySet());
			propsList.add(new PropertySetBuilder().addProperties(newProps(false)).createPropertySet());
			propMap.put("PropPropertySetList", new PropertyValue<>(PropertyDataType.PropertySetList, propsList));
		}
		return propMap;
	}

	private List<Parameter> newParams() throws SparkplugException {
		Random random = new Random();
		List<Parameter> params = new ArrayList<Parameter>();
		params.add(new Parameter("ParamInt32", ParameterDataType.Int32, random.nextInt()));
		params.add(new Parameter("ParamFloat", ParameterDataType.Float, random.nextFloat()));
		params.add(new Parameter("ParamDouble", ParameterDataType.Double, random.nextDouble()));
		params.add(new Parameter("ParamBoolean", ParameterDataType.Boolean, random.nextBoolean()));
		params.add(new Parameter("ParamString", ParameterDataType.String, newUUID()));
		return params;
	}

	private List<Metric> newComplexTemplate(boolean withTemplateDefs) throws SparkplugInvalidTypeException {
		ArrayList<Metric> metrics = new ArrayList<Metric>();
		if (withTemplateDefs) {

			// Add a new template "subType" definition with two primitive members
			metrics.add(new MetricBuilder("subType", Template,
					new TemplateBuilder().definition(true)
							.addMetric(new MetricBuilder("StringMember", String, "value").createMetric())
							.addMetric(new MetricBuilder("IntegerMember", Int32, 0).createMetric()).createTemplate())
									.createMetric());
			// Add new template "newType" definition that contains an instance of "subType" as a member
			metrics.add(new MetricBuilder("newType", Template,
					new TemplateBuilder().definition(true).addMetric(new MetricBuilder("mySubType", Template,
							new TemplateBuilder().definition(false).templateRef("subType")
									.addMetric(new MetricBuilder("StringMember", String, "value").createMetric())
									.addMetric(new MetricBuilder("IntegerMember", Int32, 0).createMetric())
									.createTemplate()).createMetric())
							.createTemplate()).createMetric());
		}

		// Add an instance of "newType
		metrics.add(new MetricBuilder("myNewType", Template,
				new TemplateBuilder().definition(false).templateRef("newType")
						.addMetric(new MetricBuilder("mySubType", Template,
								new TemplateBuilder().definition(false).templateRef("subType")
										.addMetric(new MetricBuilder("StringMember", String, "myValue").createMetric())
										.addMetric(new MetricBuilder("IntegerMember", Int32, 1).createMetric())
										.createTemplate()).createMetric())
						.createTemplate()).createMetric());

		return metrics;

	}

	private Template newTemplate(boolean isDef, String templatRef) throws SparkplugException {
		Random random = new Random();
		List<Metric> metrics = new ArrayList<Metric>();
		metrics.add(new MetricBuilder("MyInt8", Int8, (byte) random.nextInt()).createMetric());
		metrics.add(new MetricBuilder("MyInt16", Int16, (short) random.nextInt()).createMetric());
		metrics.add(new MetricBuilder("MyInt32", Int32, random.nextInt()).createMetric());
		metrics.add(new MetricBuilder("MyInt64", Int64, random.nextLong()).createMetric());
		metrics.add(new MetricBuilder("MyUInt8", UInt8, getRandomUInt8()).createMetric());
		metrics.add(new MetricBuilder("MyUInt16", UInt16, getRandomUInt16()).createMetric());
		metrics.add(new MetricBuilder("MyUInt32", UInt32, getRandomUInt32()).createMetric());
		metrics.add(new MetricBuilder("MyUInt64", UInt64, getRandomUInt64()).createMetric());
		metrics.add(new MetricBuilder("MyFloat", Float, random.nextFloat()).createMetric());
		metrics.add(new MetricBuilder("MyDouble", Double, random.nextDouble()).createMetric());
		metrics.add(new MetricBuilder("MyBoolean", Boolean, random.nextBoolean()).createMetric());
		metrics.add(new MetricBuilder("MyString", String, newUUID()).createMetric());
		metrics.add(new MetricBuilder("MyDateTime", DateTime, new Date()).createMetric());
		metrics.add(new MetricBuilder("MyText", Text, newUUID()).createMetric());
		metrics.add(new MetricBuilder("MyUUID", UUID, newUUID()).createMetric());

		return new TemplateBuilder().version("v1.0").templateRef(templatRef).definition(isDef)
				.addParameters(newParams()).addMetrics(metrics).createTemplate();
	}

	private DataSet newDataSet() throws SparkplugException {
		Random random = new Random();
		return new DataSetBuilder(14).addColumnName("Int8s").addColumnName("Int16s").addColumnName("Int32s")
				.addColumnName("Int64s").addColumnName("UInt8s").addColumnName("UInt16s").addColumnName("UInt32s")
				.addColumnName("UInt64s").addColumnName("Floats").addColumnName("Doubles").addColumnName("Booleans")
				.addColumnName("Strings").addColumnName("Dates").addColumnName("Texts").addType(DataSetDataType.Int8)
				.addType(DataSetDataType.Int16).addType(DataSetDataType.Int32).addType(DataSetDataType.Int64)
				.addType(DataSetDataType.UInt8).addType(DataSetDataType.UInt16).addType(DataSetDataType.UInt32)
				.addType(DataSetDataType.UInt64).addType(DataSetDataType.Float).addType(DataSetDataType.Double)
				.addType(DataSetDataType.Boolean).addType(DataSetDataType.String).addType(DataSetDataType.DateTime)
				.addType(DataSetDataType.Text)
				.addRow(new RowBuilder().addValue(new Value<Byte>(DataSetDataType.Int8, (byte) random.nextInt()))
						.addValue(new Value<Short>(DataSetDataType.Int16, (short) random.nextInt()))
						.addValue(new Value<Integer>(DataSetDataType.Int32, random.nextInt()))
						.addValue(new Value<Long>(DataSetDataType.Int64, random.nextLong()))
						.addValue(new Value<Short>(DataSetDataType.UInt8, getRandomUInt8()))
						.addValue(new Value<Integer>(DataSetDataType.UInt16, getRandomUInt16()))
						.addValue(new Value<Long>(DataSetDataType.UInt32, getRandomUInt32()))
						.addValue(new Value<BigInteger>(DataSetDataType.UInt64, getRandomUInt64()))
						.addValue(new Value<Float>(DataSetDataType.Float, random.nextFloat()))
						.addValue(new Value<Double>(DataSetDataType.Double, random.nextDouble()))
						.addValue(new Value<Boolean>(DataSetDataType.Boolean, random.nextBoolean()))
						.addValue(new Value<String>(DataSetDataType.String, newUUID()))
						.addValue(new Value<Date>(DataSetDataType.DateTime, new Date()))
						.addValue(new Value<String>(DataSetDataType.Text, newUUID())).createRow())
				.addRow(new RowBuilder().addValue(new Value<Byte>(DataSetDataType.Int8, (byte) random.nextInt()))
						.addValue(new Value<Short>(DataSetDataType.Int16, (short) random.nextInt()))
						.addValue(new Value<Integer>(DataSetDataType.Int32, random.nextInt()))
						.addValue(new Value<Long>(DataSetDataType.Int64, random.nextLong()))
						.addValue(new Value<Short>(DataSetDataType.UInt8, getRandomUInt8()))
						.addValue(new Value<Integer>(DataSetDataType.UInt16, getRandomUInt16()))
						.addValue(new Value<Long>(DataSetDataType.UInt32, getRandomUInt32()))
						.addValue(new Value<BigInteger>(DataSetDataType.UInt64, getRandomUInt64()))
						.addValue(new Value<Float>(DataSetDataType.Float, random.nextFloat()))
						.addValue(new Value<Double>(DataSetDataType.Double, random.nextDouble()))
						.addValue(new Value<Boolean>(DataSetDataType.Boolean, random.nextBoolean()))
						.addValue(new Value<String>(DataSetDataType.String, newUUID()))
						.addValue(new Value<Date>(DataSetDataType.DateTime, new Date()))
						.addValue(new Value<String>(DataSetDataType.Text, newUUID())).createRow())
				.createDataSet();
	}

	private short getRandomUInt8() {
		Random random = new Random();
		return (short) random.nextInt(256);
	}

	private int getRandomUInt16() {
		Random random = new Random();
		return random.nextInt(65536);

	}

	private long getRandomUInt32() {
		return ThreadLocalRandom.current().nextLong(4294967296L);
	}

	private BigInteger getRandomUInt64() {
		Random random = new Random();
		BigInteger minSize = new BigInteger("0");
		BigInteger maxSize = new BigInteger("18446744073709551616");
		BigInteger randomResult = new BigInteger(64, random);
		while (randomResult.compareTo(minSize) <= 0 || randomResult.compareTo(maxSize) >= 0) {
			randomResult = new BigInteger(64, random);
			System.out.println("New randomResult: " + randomResult);
		}
		return randomResult;
	}

	private class Publisher implements Runnable {

		private String topic;
		private SparkplugBPayload outboundPayload;

		public Publisher(String topic, SparkplugBPayload outboundPayload) {
			this.topic = topic;
			this.outboundPayload = outboundPayload;
		}

		public void run() {
			try {
				outboundPayload.setTimestamp(new Date());
				SparkplugBPayloadEncoder encoder = new SparkplugBPayloadEncoder();

				// Compress payload (optional)
				if (USING_COMPRESSION) {
					client.publish(topic,
							encoder.getBytes(PayloadUtil.compress(outboundPayload, compressionAlgorithm, false), false),
							0, false);
				} else {
					client.publish(topic, encoder.getBytes(outboundPayload, false), 0, false);
				}
			} catch (MqttPersistenceException e) {
				e.printStackTrace();
			} catch (MqttException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
