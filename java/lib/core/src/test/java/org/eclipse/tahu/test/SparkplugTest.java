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

package org.eclipse.tahu.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.eclipse.tahu.SparkplugException;
import org.eclipse.tahu.SparkplugInvalidTypeException;
import org.eclipse.tahu.json.JsonValidator;
import org.eclipse.tahu.message.PayloadDecoder;
import org.eclipse.tahu.message.SparkplugBPayloadDecoder;
import org.eclipse.tahu.message.SparkplugBPayloadEncoder;
import org.eclipse.tahu.message.model.DataSet;
import org.eclipse.tahu.message.model.DataSet.DataSetBuilder;
import org.eclipse.tahu.message.model.DataSetDataType;
import org.eclipse.tahu.message.model.File;
import org.eclipse.tahu.message.model.MetaData;
import org.eclipse.tahu.message.model.MetaData.MetaDataBuilder;
import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.Metric.MetricBuilder;
import org.eclipse.tahu.message.model.MetricDataType;
import org.eclipse.tahu.message.model.Parameter;
import org.eclipse.tahu.message.model.ParameterDataType;
import org.eclipse.tahu.message.model.PropertyDataType;
import org.eclipse.tahu.message.model.PropertySet;
import org.eclipse.tahu.message.model.PropertySet.PropertySetBuilder;
import org.eclipse.tahu.message.model.PropertyValue;
import org.eclipse.tahu.message.model.Row;
import org.eclipse.tahu.message.model.Row.RowBuilder;
import org.eclipse.tahu.message.model.SparkplugBPayload;
import org.eclipse.tahu.message.model.SparkplugBPayload.SparkplugBPayloadBuilder;
import org.eclipse.tahu.message.model.Template;
import org.eclipse.tahu.message.model.Template.TemplateBuilder;
import org.eclipse.tahu.message.model.Value;
import org.eclipse.tahu.util.PayloadUtil;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Sparkplug Test class for encoding and decoding sparkplug payloads
 */
public class SparkplugTest {

	/**
	 * A {@link JsonValidator} instance used for testing JSON validation.
	 */
	private JsonValidator validator;

	public SparkplugTest() {
		validator = JsonValidator.getInstance();
	}

	@DataProvider
	public Object[][] metricData() throws Exception {
		return new Object[][] { { "TestByteObject", MetricDataType.Int8, new Byte((byte) 123), null },
				{ "TestByte", MetricDataType.Int8, (byte) 123, null },
				{ "TestShortObject", MetricDataType.Int16, new Short((short) 12345), null },
				{ "TestShort", MetricDataType.Int16, (short) 12345, null },
				{ "TestIntObject", MetricDataType.Int32, new Integer(1234567890), null },
				{ "TestInt", MetricDataType.Int32, 1234567890, null },
				{ "TestLongObject", MetricDataType.Int64, new Long(12345679000L), null },
				{ "TestLong", MetricDataType.Int64, 12345679000L, null },
				{ "TestUnsignedByte", MetricDataType.UInt8, new Short((short) 123), null },
				{ "TestUnsignedShort", MetricDataType.UInt16, 12345, null },
				{ "TestUnsignedInt", MetricDataType.UInt32, new Long(1234567890), null },
				{ "TestUnsignedLong", MetricDataType.UInt64, BigInteger.valueOf(12345679000L), null },
				{ "TestFloatObject", MetricDataType.Float, new Float(1.11111111111111111e+30f), null },
				{ "TestFloat", MetricDataType.Float, 1.11111111111111111e+30f, null },
				{ "TestDoubleObject", MetricDataType.Double, new Double(1.11111111111111111e+300d), null },
				{ "TestDouble", MetricDataType.Double, 1.11111111111111111e+300d, null },
				{ "TestBooleanObject", MetricDataType.Boolean, new Boolean(true), null },
				{ "TestBoolean", MetricDataType.Boolean, true, null },
				{ "TestString", MetricDataType.String, "TEST_STRING", null },
				{ "TestDateTime", MetricDataType.DateTime, new Date(), null },
				{ "TestText", MetricDataType.Text, "TEST_TEXT", null },
				{ "TestUUID", MetricDataType.UUID, "915cac68-a20e-11e6-80f5-76304dec7eb7", null },
				{ "TestBytes", MetricDataType.Bytes, new byte[] { 0x0, 0x1, 0x2, 0x3, 0x4 }, null },
				{ "TestFile", MetricDataType.File, new File("/tmp/.testfile", new byte[] { 0x0, 0x1, 0x2, 0x3, 0x4 }),
						new MetaDataBuilder().fileType("bin").fileName("/tmp/.testfile").multiPart(false)
								.createMetaData() },
				{ "TestDataSet", MetricDataType.DataSet,
						new DataSetBuilder(5).addColumnName("Booleans").addColumnName("Int32s").addColumnName("Floats")
								.addColumnName("Dates").addColumnName("Strings").addType(DataSetDataType.Boolean)
								.addType(DataSetDataType.Int32).addType(DataSetDataType.Float)
								.addType(DataSetDataType.DateTime).addType(DataSetDataType.String)
								.addRow(new RowBuilder().addValue(new Value<Boolean>(DataSetDataType.Boolean, false))
										.addValue(new Value<Integer>(DataSetDataType.Int32, 1))
										.addValue(new Value<Float>(DataSetDataType.Float, 1.1F))
										.addValue(new Value<Date>(DataSetDataType.DateTime, new Date()))
										.addValue(new Value<String>(DataSetDataType.String, "abc")).createRow())
								.addRow(new RowBuilder().addValue(new Value<Boolean>(DataSetDataType.Boolean, true))
										.addValue(new Value<Integer>(DataSetDataType.Int32, 2))
										.addValue(new Value<Float>(DataSetDataType.Float, 1.2F))
										.addValue(new Value<Date>(DataSetDataType.DateTime, new Date()))
										.addValue(new Value<String>(DataSetDataType.String, "")).createRow())
								.addRow(new RowBuilder().addValue(new Value<Boolean>(DataSetDataType.Boolean, false))
										.addValue(new Value<Integer>(DataSetDataType.Int32, 3))
										.addValue(new Value<Float>(DataSetDataType.Float, 1.3F))
										.addValue(new Value<Date>(DataSetDataType.DateTime, null))
										.addValue(new Value<String>(DataSetDataType.String, null)).createRow())
								.createDataSet(),
						null },
				{ "NullDataSet", MetricDataType.DataSet, new DataSetBuilder(5).addColumnName("Booleans")
						.addColumnName("Int32s").addColumnName("Floats").addColumnName("Dates").addColumnName("Strings")
						.addType(DataSetDataType.Boolean).addType(DataSetDataType.Int32).addType(DataSetDataType.Float)
						.addType(DataSetDataType.DateTime).addType(DataSetDataType.String)
						.addRow(new RowBuilder().addValue(new Value<Boolean>(DataSetDataType.Boolean, null))
								.addValue(new Value<Integer>(DataSetDataType.Int32, null))
								.addValue(new Value<Float>(DataSetDataType.Float, null))
								.addValue(new Value<Date>(DataSetDataType.DateTime, null))
								.addValue(new Value<String>(DataSetDataType.String, null)).createRow())
						.createDataSet(), null },
				{ "TestTemplateDef", MetricDataType.Template, new TemplateBuilder().version("v1.0").templateRef(null)
						.definition(true).addParameter(new Parameter("BoolParam", ParameterDataType.Boolean, true))
						.addParameter(new Parameter("IntParam", ParameterDataType.Int32, 12345678))
						.addParameter(new Parameter("DateParam", ParameterDataType.DateTime, new Date()))
						.addMetric(new MetricBuilder("TemplateMetric1", MetricDataType.Boolean, true).createMetric())
						.addMetric(new MetricBuilder("TemplateMetric2", MetricDataType.Int32, 1234567890)
								.createMetric())
						.addMetric(
								new MetricBuilder("TemplateMetric3", MetricDataType.String,
										"TEST_STRING")
												.properties(
														new PropertySetBuilder()
																.addProperty("prop1a",
																		new PropertyValue<>(PropertyDataType.Float,
																				1.23F))
																.addProperty("prop1b",
																		new PropertyValue<>(PropertyDataType.Float,
																				new Float(1.23)))
																.addProperty("prop2",
																		new PropertyValue<>(PropertyDataType.DateTime,
																				new Date()))
																.addProperty("prop3",
																		new PropertyValue<>(PropertyDataType.Text,
																				"PROP3_TEXT"))
																.addProperty("prop4",
																		new PropertyValue<>(PropertyDataType.String,
																				null))
																.createPropertySet())
												.createMetric())
						.createTemplate(), null },
				{ "TestTemplateInst", MetricDataType.Template, new TemplateBuilder().version("v1.0")
						.templateRef("TestTemplateDef").definition(true)
						.addParameter(new Parameter("BoolParam", ParameterDataType.Boolean, true))
						.addParameter(new Parameter("IntParam", ParameterDataType.Int32, 1234567890))
						.addParameter(new Parameter("DateParam", ParameterDataType.DateTime, new Date()))
						.addMetric(new MetricBuilder("TemplateMetric1", MetricDataType.Boolean, true).createMetric())
						.addMetric(
								new MetricBuilder("TemplateMetric2", MetricDataType.Int32, 1234567890).createMetric())
						.addMetric(new MetricBuilder("TemplateMetric3", MetricDataType.String, "TEST_STRING")
								.createMetric())
						.createTemplate(), null } };
	}

	@DataProvider
	public Object[][] metricFieldsData() throws Exception {
		return new Object[][] {
				{ new MetricBuilder("metric1", MetricDataType.Int32, 1234).alias(12345L)
						.timestamp(new Date(1479424852194L)).isHistorical(true).isTransient(false).createMetric(),
						12345L, new Date(1479424852194L), true, false, false },
				{ new MetricBuilder("metric2", MetricDataType.DateTime, null).alias(1L)
						.timestamp(new Date(1479421234564L)).isHistorical(true).isTransient(true).createMetric(), 1L,
						new Date(1479421234564L), true, true, true },
				{ new MetricBuilder("metric3", MetricDataType.String, "Test").alias(999999999L)
						.timestamp(new Date(1479123452194L)).isHistorical(false).isTransient(false).createMetric(),
						999999999L, new Date(1479123452194L), false, false, false },
				{ new MetricBuilder("metric4", MetricDataType.BooleanArray,
						new Boolean[] { false, true, false, true, false, true, false, true, false, true, false, true,
								false, true, false, true, false, true, false, true }).alias(999999999L)
										.timestamp(new Date(1479123452194L)).isHistorical(false).isTransient(false)
										.createMetric(),
						999999999L, new Date(1479123452194L), false, false, false }, };

	}

	@DataProvider
	public Object[][] invalidParamterTypeData() throws Exception {
		return new Object[][] { { "Param1", ParameterDataType.Boolean, 12345 },
				{ "Param2", ParameterDataType.Int8, true }, { "Param3", ParameterDataType.Int16, "123" },
				{ "Param4", ParameterDataType.Int32, true }, { "Param5", ParameterDataType.Int64, new Date() },
				{ "Param6", ParameterDataType.DateTime, 12345 }, { "Param7", ParameterDataType.Text, 12345 },
				{ "Param8", ParameterDataType.String, 12345 }, };
	}

	@DataProvider
	public Object[][] invalidMetricDataType() throws Exception {
		return new Object[][] { { "TestByteObject", MetricDataType.Int8, new Short((short) 123), null },
				{ "TestByte", MetricDataType.Int8, new Long(12345679000L), null },
				{ "TestShortObject", MetricDataType.Int16, new Float(12345), null },
				{ "TestBooleanObject", MetricDataType.Boolean, new Double(1.11111111111111111e+300d), null },
				{ "TestBoolean", MetricDataType.Boolean, 123, null },
				{ "TestString", MetricDataType.String, true, null },
				{ "TestDateTime", MetricDataType.DateTime, 1234567, null },
				{ "TestUUID", MetricDataType.UUID, 998877, null },
				{ "TestBytes", MetricDataType.Bytes, new int[] { 1, 2, 3, 4, 5 }, null } };
	}

	@Test
	public void testEnDeCode() throws SparkplugInvalidTypeException {
		Date currentTime = new Date();
		SparkplugBPayloadBuilder payloadBuilder = new SparkplugBPayloadBuilder().setTimestamp(currentTime).setSeq(0L)
				.setUuid("123456789").setBody("Hello".getBytes());

		// Create MetaData
		MetaData metaData = new MetaDataBuilder().contentType("none").size(12L).seq(0L).fileName("none")
				.fileType("none").md5("none").multiPart(false).description("none").createMetaData();

		// Create one metric
		payloadBuilder.addMetric(new MetricBuilder("Name", MetricDataType.Int8, (byte) 65).alias(0L)
				.timestamp(currentTime).isHistorical(false).metaData(metaData).createMetric());

		// Create null metric
		payloadBuilder.addMetric(new MetricBuilder("Null", MetricDataType.String, null).alias(0L).timestamp(currentTime)
				.isHistorical(false).metaData(metaData).createMetric());

		// Encode
		SparkplugBPayloadEncoder encoder = new SparkplugBPayloadEncoder();
		byte[] bytes = null;
		try {
			bytes = encoder.getBytes(payloadBuilder.createPayload(), false);
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

		// Decode
		PayloadDecoder<SparkplugBPayload> decoder = new SparkplugBPayloadDecoder();
		SparkplugBPayload decodedPayload = null;
		try {
			decodedPayload = decoder.buildFromByteArray(bytes, null);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

		// SparkplugBPayload checks
		assertThat(currentTime).isEqualTo(decodedPayload.getTimestamp());
		assertThat(0L).isEqualTo(decodedPayload.getSeq());
		assertThat("123456789").isEqualTo(decodedPayload.getUuid());
		assertThat(Arrays.equals("Hello".getBytes(), decodedPayload.getBody())).isTrue();

		// Test the Metric
		assertThat(2).isEqualTo(decodedPayload.getMetrics().size());
		Metric decodedMetric = decodedPayload.getMetrics().get(0);
		assertThat("Name").isEqualTo(decodedMetric.getName());
		assertThat(new Long(0)).isEqualTo(decodedMetric.getAlias());
		assertThat(currentTime).isEqualTo(decodedMetric.getTimestamp());
		assertThat(MetricDataType.Int8).isEqualTo(decodedMetric.getDataType());
		assertThat(Boolean.FALSE).isEqualTo(decodedMetric.isHistorical());
		assertThat((byte) 65).isEqualTo(decodedMetric.getValue());
		assertThat(decodedMetric.getMetaData()).isNotNull();

		// Test the MetaData
		MetaData decodedMetaData = decodedMetric.getMetaData();
		assertThat(metaData).isEqualTo(decodedMetric.getMetaData());
		assertThat("none").isEqualTo(decodedMetaData.getContentType());
		assertThat(12L).isEqualTo(decodedMetaData.getSize());
		assertThat(0L).isEqualTo(decodedMetaData.getSeq());
		assertThat("none").isEqualTo(decodedMetaData.getFileName());
		assertThat("none").isEqualTo(decodedMetaData.getFileType());
		assertThat("none").isEqualTo(decodedMetaData.getMd5());
		assertThat("none").isEqualTo(decodedMetaData.getDescription());
	}

	@Test(
			dataProvider = "metricData")
	public void testValidMetricPayload(String name, MetricDataType type, Object value, MetaData metaData)
			throws SparkplugException {
		testMetricPayload(name, type, value, metaData);
	}

	@Test(
			dataProvider = "invalidMetricDataType",
			expectedExceptions = SparkplugInvalidTypeException.class)
	public void testInvalidMetricDataType(String name, MetricDataType type, Object value, MetaData metaData)
			throws SparkplugException {
		testMetricPayload(name, type, value, metaData);
	}

	@Test(
			dataProvider = "invalidParamterTypeData",
			expectedExceptions = SparkplugInvalidTypeException.class)
	public void testInvalidParameterDataType(String name, ParameterDataType type, Object value)
			throws SparkplugInvalidTypeException {
		new Parameter(name, type, value);
	}

	@Test(
			dataProvider = "metricFieldsData")
	public void testValidMetricPayload(Metric metric, long alias, Date timestamp, boolean isHistorical,
			boolean isTransient, boolean isNull) throws Exception {
		// Encode
		byte[] bytes = new SparkplugBPayloadEncoder().getBytes(
				new SparkplugBPayloadBuilder().setTimestamp(new Date()).addMetric(metric).createPayload(), false);

		// Decode and test
		SparkplugBPayload payload = new SparkplugBPayloadDecoder().buildFromByteArray(bytes, null);
		Metric decodedMetric = payload.getMetrics().get(0);
		assertThat(decodedMetric.getAlias()).isEqualTo(alias);
		assertThat(decodedMetric.getTimestamp()).isEqualTo(timestamp);
		assertThat(decodedMetric.isHistorical()).isEqualTo(isHistorical);
		assertThat(decodedMetric.isTransient()).isEqualTo(isTransient);
		assertThat(decodedMetric.isNull()).isEqualTo(isNull);
		System.out.println("JSON: " + PayloadUtil.toJsonString(payload));
	}

	@Test(
			dataProvider = "metricData")
	public void testJsonValidation(String name, MetricDataType type, Object value, MetaData metaData)
			throws SparkplugException, Exception {
		Date currentTime = new Date();

		SparkplugBPayload payload = new SparkplugBPayloadBuilder().setTimestamp(currentTime)
				.addMetric(new MetricBuilder(name, type, value).metaData(metaData).createMetric()).createPayload();

//    	assertThat(validator.isJsonValid(PayloadUtil.toJsonString(payload))).isTrue();
	}

	private void testMetricPayload(String name, MetricDataType type, Object value, MetaData metaData)
			throws SparkplugException {
		try {
			SparkplugBPayloadEncoder encoder = new SparkplugBPayloadEncoder();
			Date currentTime = new Date();

			// Encode
			byte[] bytes = encoder.getBytes(new SparkplugBPayloadBuilder().setTimestamp(currentTime)
					.addMetric(new MetricBuilder(name, type, value).metaData(metaData).createMetric()).createPayload(),
					false);

			// Decode
			PayloadDecoder<SparkplugBPayload> decoder = new SparkplugBPayloadDecoder();
			SparkplugBPayload decodedPayload = decoder.buildFromByteArray(bytes, null);

			for (Metric metric : decodedPayload.getMetrics()) {
				if (metric.getDataType().equals(MetricDataType.Template)) {
					System.out.println("PAYLOAD: " + PayloadUtil.toJsonString(decodedPayload));
					break;
				}
			}

			// SparkplugBPayload checks
			assertThat(currentTime).isEqualTo(decodedPayload.getTimestamp());
			assertNull(decodedPayload.getSeq());
			assertThat(decodedPayload.getBody()).isNull();

			// Metric checks
			assertThat(1).isEqualTo(decodedPayload.getMetrics().size());

			doMetricTests(new MetricBuilder(name, type, value).metaData(metaData).createMetric(),
					decodedPayload.getMetrics().get(0));
		} catch (SparkplugException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private void doMetricTests(Metric metric, Metric decodedMetric) throws Exception {
		String name = metric.getName();
		MetricDataType type = metric.getDataType();
		Object value = metric.getValue();
		MetaData metaData = metric.getMetaData();
		PropertySet propertySet = metric.getProperties();

		assertThat(name).isEqualTo(decodedMetric.getName());
		assertThat(type).isEqualTo(decodedMetric.getDataType());
		assertThat(Boolean.FALSE).isEqualTo(decodedMetric.isHistorical());
		assertThat(Boolean.FALSE).isEqualTo(decodedMetric.isTransient());

		// Test PropertySet
		if (propertySet != null) {
			Map<String, PropertyValue<? extends PropertyDataType>> map = propertySet.getPropertyMap();
			Map<String, PropertyValue<? extends PropertyDataType>> decodedMap =
					decodedMetric.getProperties().getPropertyMap();
			assertThat(map.size()).isEqualTo(decodedMap.size());
			for (String key : map.keySet()) {
				assertThat(map.get(key)).isEqualTo(decodedMap.get(key));
			}
		}

		// Test the value
		if (type.toIntValue() == MetricDataType.Bytes.toIntValue()) {
			compareBytes((byte[]) value, (byte[]) decodedMetric.getValue());
			assertThat(decodedMetric.getMetaData()).isNull();
		} else if (type.toIntValue() == MetricDataType.File.toIntValue()) {
			File someFile = (File) value;
			File decodedFile = (File) decodedMetric.getValue();
			compareBytes(someFile.getBytes(), decodedFile.getBytes());
			assertThat(someFile.getFileName()).isEqualTo(decodedFile.getFileName());
			assertThat(decodedMetric.getMetaData()).isNotNull();
			assertThat(metaData.getFileName()).isEqualTo(decodedMetric.getMetaData().getFileName());
			assertThat(metaData.isMultiPart()).isEqualTo(decodedMetric.getMetaData().isMultiPart());
			assertThat(metaData.getFileType()).isEqualTo(decodedMetric.getMetaData().getFileType());
		} else if (type.toIntValue() == MetricDataType.DataSet.toIntValue()) {
			// Tests for DataSets
			DataSet dataSet = (DataSet) value;
			DataSet decodedDataSet = (DataSet) decodedMetric.getValue();
			List<String> columnNames = dataSet.getColumnNames();
			List<String> decodedColumnNames = decodedDataSet.getColumnNames();
			List<DataSetDataType> types = dataSet.getTypes();
			List<DataSetDataType> decodedTypes = decodedDataSet.getTypes();
			List<Row> rows = dataSet.getRows();
			List<Row> decodedRows = decodedDataSet.getRows();
			assertThat(dataSet.getNumOfColumns()).isEqualTo(decodedDataSet.getNumOfColumns());
			// Test Columns
			for (int i = 0; i < columnNames.size(); i++) {
				assertThat(columnNames.get(i)).isEqualTo(decodedColumnNames.get(i));
			}
			// Test Types
			for (int i = 0; i < types.size(); i++) {
				assertThat(types.get(i)).isEqualTo(decodedTypes.get(i));
			}
			// Test Rows
			for (int i = 0; i < rows.size(); i++) {
				List<Value<?>> values = rows.get(i).getValues();
				List<Value<?>> decodedValues = decodedRows.get(i).getValues();
				assertThat(values.size()).isEqualTo(decodedValues.size());
				for (int j = 0; j < values.size(); j++) {
					Value<?> rowValue = values.get(j);
					Value<?> decodedValue = decodedValues.get(j);
					assertThat(rowValue.getType()).isEqualTo(decodedValue.getType());
					assertThat(rowValue.getValue()).isEqualTo(decodedValue.getValue());
				}
			}
		} else if (type.toIntValue() == MetricDataType.Template.toIntValue()) {
			// Tests for Templates
			Template template = (Template) value;
			Template decodedTemplate = (Template) decodedMetric.getValue();
			List<Parameter> parameters = template.getParameters();
			List<Parameter> decodedParameters = decodedTemplate.getParameters();
			List<Metric> metrics = template.getMetrics();
			List<Metric> decodedMetrics = decodedTemplate.getMetrics();
			// Test Parameters
			assertThat(parameters.size()).isEqualTo(decodedParameters.size());
			for (int i = 0; i < parameters.size(); i++) {
				assertThat(parameters.get(i)).isEqualTo(decodedParameters.get(i));
			}
			// Test Metrics
			for (int i = 0; i < metrics.size(); i++) {
				doMetricTests(metrics.get(i), decodedMetrics.get(i));
			}
		} else {
			// Tests for all other types
			assertThat(value).isEqualTo(decodedMetric.getValue());
			assertThat(decodedMetric.getMetaData()).isNull();
		}
	}

	private void compareBytes(byte[] bytes1, byte[] bytes2) {
		assertThat(bytes1.length).isEqualTo(bytes2.length);
		for (int i = 0; i < bytes1.length; i++) {
			assertThat(bytes1[i]).isEqualTo(bytes2[i]);
		}
	}
}
