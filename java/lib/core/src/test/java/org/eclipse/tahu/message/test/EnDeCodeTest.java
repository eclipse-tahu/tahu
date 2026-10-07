/*
 * Licensed Materials - Property of Cirrus Link Solutions
 * Copyright (c) 2023 Cirrus Link Solutions LLC - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 */
package org.eclipse.tahu.message.test;

import org.eclipse.tahu.message.SparkplugBPayloadDecoder;
import org.eclipse.tahu.message.SparkplugBPayloadEncoder;
import org.eclipse.tahu.message.model.Metric.MetricBuilder;
import org.eclipse.tahu.message.model.MetricDataType;
import org.eclipse.tahu.message.model.SparkplugBPayload;
import org.eclipse.tahu.message.model.SparkplugBPayload.SparkplugBPayloadBuilder;

import junit.framework.TestCase;

public class EnDeCodeTest extends TestCase {

	public EnDeCodeTest(String testName) {
		super(testName);
	}

	public void testSimple() {
		assertTrue(true);
	}

	public void testEncodeDecode() {
		try {
			SparkplugBPayload originalPayload = new SparkplugBPayloadBuilder()
					.addMetric(
							new MetricBuilder("String", MetricDataType.String, "日本人 中國的 ~=[]()%+{}@;").createMetric())
					.addMetric(new MetricBuilder("StringArray", MetricDataType.StringArray,
							new String[] { "日本人 中國的 ~=[]()%+{}@;" }).createMetric())
					.addMetric(new MetricBuilder("StringArray", MetricDataType.StringArray,
							new String[] { "日本人 中國的 ~=[]()%+{}@;", "漢字" }).createMetric())
					.addMetric(new MetricBuilder("StringArray", MetricDataType.StringArray,
							new String[] { "漢字", "日本人 中國的 ~=[]()%+{}@;" }).createMetric())
					.addMetric(new MetricBuilder("StringArray", MetricDataType.StringArray,
							new String[] { "ناطرريننهمم ع نااارررر", "漢字", "日本人 中國的 ~=[]()%+{}@;" }).createMetric())
					.createPayload();
			byte[] encoded = new SparkplugBPayloadEncoder().getBytes(originalPayload, false);

			SparkplugBPayload decodedPayload = new SparkplugBPayloadDecoder().buildFromByteArray(encoded, null);

			assertEquals(((String[]) originalPayload.getMetrics().get(1).getValue())[0],
					((String[]) decodedPayload.getMetrics().get(1).getValue())[0]);

			assertEquals(((String[]) originalPayload.getMetrics().get(2).getValue())[0],
					((String[]) decodedPayload.getMetrics().get(2).getValue())[0]);
			assertEquals(((String[]) originalPayload.getMetrics().get(2).getValue())[1],
					((String[]) decodedPayload.getMetrics().get(2).getValue())[1]);

			assertEquals(((String[]) originalPayload.getMetrics().get(3).getValue())[0],
					((String[]) decodedPayload.getMetrics().get(3).getValue())[0]);
			assertEquals(((String[]) originalPayload.getMetrics().get(3).getValue())[1],
					((String[]) decodedPayload.getMetrics().get(3).getValue())[1]);

			assertEquals(((String[]) originalPayload.getMetrics().get(4).getValue())[0],
					((String[]) decodedPayload.getMetrics().get(4).getValue())[0]);
			assertEquals(((String[]) originalPayload.getMetrics().get(4).getValue())[1],
					((String[]) decodedPayload.getMetrics().get(4).getValue())[1]);
			assertEquals(((String[]) originalPayload.getMetrics().get(4).getValue())[2],
					((String[]) decodedPayload.getMetrics().get(4).getValue())[2]);
		} catch (Exception e) {
			System.out.println(e);
			fail();
		}
	}

	public void testUInt8ArrayEncodeDecode() {
		try {
			Short[] fullRange = new Short[256];
			for (int i = 0; i < 256; i++) {
				fullRange[i] = (short) i;
			}

			SparkplugBPayload originalPayload = new SparkplugBPayloadBuilder()
					.addMetric(new MetricBuilder("boundaries", MetricDataType.UInt8Array,
							new Short[] { 0, 1, 127, 128, 200, 254, 255 }).createMetric())
					.addMetric(new MetricBuilder("fullRange", MetricDataType.UInt8Array, fullRange).createMetric())
					.addMetric(new MetricBuilder("single255", MetricDataType.UInt8Array, new Short[] { 255 })
							.createMetric())
					.addMetric(new MetricBuilder("single128", MetricDataType.UInt8Array, new Short[] { 128 })
							.createMetric())
					.addMetric(new MetricBuilder("empty", MetricDataType.UInt8Array, new Short[] {}).createMetric())
					.addMetric(new MetricBuilder("uint16", MetricDataType.UInt16Array,
							new Integer[] { 0, 32767, 32768, 65535 }).createMetric())
					.createPayload();
			byte[] encoded = new SparkplugBPayloadEncoder().getBytes(originalPayload, false);

			SparkplugBPayload decodedPayload = new SparkplugBPayloadDecoder().buildFromByteArray(encoded, null);

			for (int metric = 0; metric < 5; metric++) {
				Short[] expected = (Short[]) originalPayload.getMetrics().get(metric).getValue();
				Short[] actual = (Short[]) decodedPayload.getMetrics().get(metric).getValue();
				assertEquals(expected.length, actual.length);
				for (int i = 0; i < expected.length; i++) {
					assertEquals(expected[i], actual[i]);
					assertTrue(actual[i] >= 0 && actual[i] <= 255);
				}
			}

			Integer[] expected16 = (Integer[]) originalPayload.getMetrics().get(5).getValue();
			Integer[] actual16 = (Integer[]) decodedPayload.getMetrics().get(5).getValue();
			assertEquals(expected16.length, actual16.length);
			for (int i = 0; i < expected16.length; i++) {
				assertEquals(expected16[i], actual16[i]);
			}
		} catch (Exception e) {
			System.out.println(e);
			fail();
		}
	}
}
