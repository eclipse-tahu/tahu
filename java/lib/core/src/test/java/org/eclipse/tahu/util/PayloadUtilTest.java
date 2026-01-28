/********************************************************************************
 * Copyright (c) 2016-2022 Cirrus Link Solutions and others
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

package org.eclipse.tahu.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.Metric.MetricBuilder;
import org.eclipse.tahu.message.model.MetricDataType;
import org.eclipse.tahu.message.model.SparkplugBPayload;
import org.eclipse.tahu.message.model.SparkplugBPayload.SparkplugBPayloadBuilder;
import org.eclipse.tahu.message.model.Template;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests for PayloadUtil.
 */
public class PayloadUtilTest {

	private Date testTime;

	public PayloadUtilTest() {
		this.testTime = new Date();
	}

	@DataProvider
	public Object[][] compressionData() throws Exception {
		return new Object[][] {
				{ CompressionAlgorithm.DEFLATE,
						new SparkplugBPayloadBuilder().setTimestamp(testTime).setSeq(0L).setUuid("123456789")
								.setBody("Hello".getBytes())
								.addMetric(
										new MetricBuilder("TestInt", MetricDataType.Int32, 1234567890).createMetric())
								.createPayload() },
				{ CompressionAlgorithm.GZIP,
						new SparkplugBPayloadBuilder().setTimestamp(testTime).setSeq(0L).setUuid("123456789")
								.setBody("Hello".getBytes())
								.addMetric(
										new MetricBuilder("TestInt", MetricDataType.Int32, 1234567890).createMetric())
								.createPayload() } };
	}

	@Test(
			dataProvider = "compressionData")
	public void testCompression(CompressionAlgorithm algorithm, SparkplugBPayload payload) throws Exception {

		// Compress the payload
		SparkplugBPayload compressedPayload = PayloadUtil.compress(payload, algorithm, false);

		// Test that there is a body (the compressed bytes)
		assertThat(compressedPayload.getBody() != null).isTrue();

		// Test that the sequence number is the same
		assertThat(compressedPayload.getSeq()).isEqualTo(payload.getSeq());

		// Test that the UUID is set correctly
		assertThat(compressedPayload.getUuid()).isEqualTo(PayloadUtil.UUID_COMPRESSED);

		// Decompress the payload
		SparkplugBPayload decompressedPayload = PayloadUtil.decompress(compressedPayload, null);

		// Test that the decompressed payload matches the original
		assertThat(decompressedPayload.getTimestamp()).isEqualTo(payload.getTimestamp());
		assertThat(decompressedPayload.getSeq()).isEqualTo(payload.getSeq());
		assertThat(decompressedPayload.getUuid()).isEqualTo(payload.getUuid());
		assertThat(Arrays.equals(decompressedPayload.getBody(), payload.getBody())).isTrue();
		// Test metrics
		List<Metric> decompressedMetrics = decompressedPayload.getMetrics();
		List<Metric> metrics = payload.getMetrics();
		for (int i = 0; i < metrics.size(); i++) {
			Metric decompressedMetric = decompressedMetrics.get(i);
			Metric metric = metrics.get(i);
			assertThat(decompressedMetric.getName()).isEqualTo(metric.getName());
			assertThat(decompressedMetric.getValue()).isEqualTo(metric.getValue());
			assertThat(decompressedMetric.getDataType()).isEqualTo(metric.getDataType());
		}

	}

	@DataProvider
	public Object[][] jsonDeserializeData() throws Exception {
		return new Object[][] { { "{\n" + "  \"timestamp\" : 1766420430879,\n" + "  \"metrics\" : [ {\n"
				+ "    \"name\" : \"Test UDT\",\n" + "    \"timestamp\" : 1766420430878,\n"
				+ "    \"dataType\" : \"Template\",\n" + "    \"metaData\" : {\n"
				+ "      \"md5\" : \"a6254ee7db0ae8bd854d4eb296aa78ee\"\n" + "    },\n" + "    \"value\" : {\n"
				+ "      \"isDefinition\" : true,\n" + "      \"metrics\" : [ {\n"
				+ "        \"name\" : \"UDTOPCTag\",\n" + "        \"timestamp\" : 1766420429756,\n"
				+ "        \"dataType\" : \"Int32\",\n" + "        \"metaData\" : { },\n" + "        \"value\" : null\n"
				+ "      }, {\n" + "        \"name\" : \"UDTMemoryTag\",\n" + "        \"timestamp\" : 1766420429757,\n"
				+ "        \"dataType\" : \"Int32\",\n" + "        \"metaData\" : { },\n" + "        \"value\" : null\n"
				+ "      } ]\n" + "    }\n" + "  }, {\n" + "    \"name\" : \"Node Control/Next Server\",\n"
				+ "    \"timestamp\" : 1766420430881,\n" + "    \"dataType\" : \"Boolean\",\n"
				+ "    \"properties\" : {\n" + "      \"documentation\" : {\n" + "        \"type\" : \"String\",\n"
				+ "        \"value\" : \"Writeable tag to request the Edge Node to walk to the next MQTT server\"\n"
				+ "      }\n" + "    },\n" + "    \"value\" : false\n" + "  }, {\n"
				+ "    \"name\" : \"Node Info/Transmission Version\",\n" + "    \"timestamp\" : 1766420430881,\n"
				+ "    \"dataType\" : \"String\",\n" + "    \"properties\" : {\n" + "      \"documentation\" : {\n"
				+ "        \"type\" : \"String\",\n"
				+ "        \"value\" : \"The version of MQTT Transmission installed at the Edge Node\"\n" + "      }\n"
				+ "    },\n" + "    \"value\" : \"4.0.33-SNAPSHOT (b2025121922)\"\n" + "  }, {\n"
				+ "    \"name\" : \"bdSeq\",\n" + "    \"timestamp\" : 1766420430880,\n"
				+ "    \"dataType\" : \"Int64\",\n" + "    \"value\" : 135\n" + "  }, {\n"
				+ "    \"name\" : \"Node Control/Rebirth\",\n" + "    \"timestamp\" : 1766420430881,\n"
				+ "    \"dataType\" : \"Boolean\",\n" + "    \"properties\" : {\n" + "      \"documentation\" : {\n"
				+ "        \"type\" : \"String\",\n"
				+ "        \"value\" : \"Writeable tag to request the Edge Node to resend its cached NBIRTH and DBIRTH messages without disconnecting or sending DEATH messages first\"\n"
				+ "      }\n" + "    },\n" + "    \"value\" : false\n" + "  } ],\n" + "  \"seq\" : 0\n" + "}" },
				{ "{\"timestamp\":1766508645412,\"metrics\":[{\"name\":\"M1\",\"timestamp\":1766508643756,\"dataType\":\"Template\",\"value\":{\"reference\":\"Test UDT\",\"isDefinition\":false,\"metrics\":[{\"name\":\"UDTOPCTag\",\"timestamp\":1766508643756,\"dataType\":\"Int32\",\"value\":7}]}}],\"seq\":11}" } };
	}

	@Test(
			dataProvider = "jsonDeserializeData")
	public void testJsonDeserialize(String jsonPayload) throws Exception {
		SparkplugBPayload sparkplugPayload = PayloadUtil.fromJsonString(jsonPayload);

		for (Metric metric : sparkplugPayload.getMetrics()) {
			if (MetricDataType.Template.equals(metric.getDataType())) {
				System.out.println("Template: " + metric.getDataType());
			}

			if (MetricDataType.Template.equals(metric.getDataType()) && ((Template) metric.getValue()).isDefinition()) {
				System.out.println("Valid type and definition? " + ((Template) metric.getValue()).isDefinition());
			}
		}

		System.out.println("sparkplugPayload: " + sparkplugPayload);
	}
}
