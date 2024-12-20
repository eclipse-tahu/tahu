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

package org.eclipse.tahu.message;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.tahu.SparkplugInvalidTypeException;
import org.eclipse.tahu.message.model.DataSet.DataSetBuilder;
import org.eclipse.tahu.message.model.DataSetDataType;
import org.eclipse.tahu.message.model.File;
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
import org.eclipse.tahu.model.MetricDataTypeMap;
import org.eclipse.tahu.protobuf.SparkplugBProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PayloadDecode} implementation for decoding Sparkplug B payloads.
 */
public class SparkplugBPayloadDecoder implements PayloadDecoder<SparkplugBPayload> {

	private static final Logger logger = LoggerFactory.getLogger(SparkplugBPayloadDecoder.class.getName());

	/**
	 * Default Constructor
	 */
	public SparkplugBPayloadDecoder() {
		super();
	}

	@Override
	public SparkplugBPayload buildFromByteArray(byte[] bytes, MetricDataTypeMap metricDataTypeMap) throws Exception {
		SparkplugBProto.Payload protoPayload = SparkplugBProto.Payload.parseFrom(bytes);
		SparkplugBPayloadBuilder builder = new SparkplugBPayloadBuilder();

		// Set the timestamp
		if (protoPayload.hasTimestamp()) {
			builder.setTimestamp(new Date(protoPayload.getTimestamp()));
		}

		// Set the sequence number
		if (protoPayload.hasSeq()) {
			builder.setSeq(protoPayload.getSeq());
		}

		// Set the Metrics
		for (SparkplugBProto.Payload.Metric protoMetric : protoPayload.getMetricsList()) {
			builder.addMetric(convertMetric(protoMetric, metricDataTypeMap, null));
		}

		// Set the body
		if (protoPayload.hasBody()) {
			builder.setBody(protoPayload.getBody().toByteArray());
		}

		// Set the body
		if (protoPayload.hasUuid()) {
			builder.setUuid(protoPayload.getUuid());
		}

		return builder.createPayload();
	}

	private Metric convertMetric(SparkplugBProto.Payload.Metric protoMetric, MetricDataTypeMap metricDataTypeMap,
			String prefix) throws Exception {
		// Convert the dataType
		MetricDataType dataType = MetricDataType.fromInteger((protoMetric.getDatatype()));
		if (dataType == MetricDataType.Unknown) {
			if (metricDataTypeMap != null && !metricDataTypeMap.isEmpty()) {
				if (protoMetric.hasName()) {
					dataType = metricDataTypeMap
							.getMetricDataType(prefix != null ? prefix + protoMetric.getName() : protoMetric.getName());
				} else if (protoMetric.hasAlias()) {
					dataType = metricDataTypeMap.getMetricDataType(protoMetric.getAlias());
				} else {
					logger.error("Failed to decode the payload on metric: {}", protoMetric);
					return null;
				}
			} else {
				logger.error("Failed to decode the payload on metric datatype: {}", protoMetric);
				return null;
			}
		}

		// Build and return the Metric
		return new MetricBuilder(protoMetric.hasName() ? protoMetric.getName() : null, dataType,
				getMetricValue(protoMetric, metricDataTypeMap, prefix))
						.isHistorical(protoMetric.hasIsHistorical() ? protoMetric.getIsHistorical() : null)
						.isTransient(
								protoMetric.hasIsTransient() ? protoMetric.getIsTransient() : null)
						.timestamp(protoMetric
								.hasTimestamp() ? new Date(protoMetric.getTimestamp()) : null)
						.alias(protoMetric.hasAlias() ? protoMetric.getAlias() : null)
						.metaData(protoMetric.hasMetadata()
								? new MetaDataBuilder().contentType(protoMetric.getMetadata().getContentType())
										.size(protoMetric.getMetadata().getSize())
										.seq(protoMetric.getMetadata().getSeq())
										.fileName(protoMetric.getMetadata().getFileName())
										.fileType(protoMetric.getMetadata().getFileType())
										.md5(protoMetric.getMetadata().getMd5())
										.multiPart(protoMetric.getMetadata().getIsMultiPart())
										.description(protoMetric.getMetadata().getDescription()).createMetaData()
								: null)
						.properties(protoMetric.hasProperties()
								? new PropertySetBuilder().addProperties(convertProperties(protoMetric.getProperties()))
										.createPropertySet()
								: null)
						.createMetric();
	}

	protected Map<String, PropertyValue<? extends PropertyDataType>> convertProperties(
			SparkplugBProto.Payload.PropertySet decodedPropSet) throws SparkplugInvalidTypeException, Exception {
		Map<String, PropertyValue<? extends PropertyDataType>> map =
				new HashMap<String, PropertyValue<? extends PropertyDataType>>();
		List<String> keys = decodedPropSet.getKeysList();
		List<SparkplugBProto.Payload.PropertyValue> values = decodedPropSet.getValuesList();
		for (int i = 0; i < keys.size(); i++) {
			SparkplugBProto.Payload.PropertyValue value = values.get(i);
			map.put(keys.get(i),
					new PropertyValue<>(PropertyDataType.fromInteger(value.getType()), getPropertyValue(value)));
		}
		return map;
	}

	protected Object getPropertyValue(SparkplugBProto.Payload.PropertyValue value) throws Exception {
		PropertyDataType type = PropertyDataType.fromInteger(value.getType());
		if (value.getIsNull()) {
			return null;
		}

		if (type.toIntValue() == PropertyDataType.Boolean.toIntValue()) {
			return value.getBooleanValue();
		} else if (type.toIntValue() == PropertyDataType.DateTime.toIntValue()) {
			return new Date(value.getLongValue());
		} else if (type.toIntValue() == PropertyDataType.Double.toIntValue()) {
			return value.getDoubleValue();
		} else if (type.toIntValue() == PropertyDataType.Float.toIntValue()) {
			return value.getFloatValue();
		} else if (type.toIntValue() == PropertyDataType.Int8.toIntValue()) {
			return (byte) value.getIntValue();
		} else if (type.toIntValue() == PropertyDataType.Int16.toIntValue()) {
			return (short) value.getIntValue();
		} else if (type.toIntValue() == PropertyDataType.Int32.toIntValue()) {
			return value.getIntValue();
		} else if (type.toIntValue() == PropertyDataType.Int64.toIntValue()) {
			return value.getLongValue();
		} else if (type.toIntValue() == PropertyDataType.UInt8.toIntValue()) {
			return (short) value.getIntValue();
		} else if (type.toIntValue() == PropertyDataType.UInt16.toIntValue()) {
			return value.getIntValue();
		} else if (type.toIntValue() == PropertyDataType.UInt32.toIntValue()) {
			if (value.hasIntValue()) {
				return Integer.toUnsignedLong(value.getIntValue());
			} else if (value.hasLongValue()) {
				return value.getLongValue();
			} else {
				throw new Exception("Failed to decode: Invalid value for UInt32 datatype " + type);
			}
		} else if (type.toIntValue() == PropertyDataType.UInt64.toIntValue()) {
			return new BigInteger(Long.toUnsignedString(value.getLongValue()));
		} else if (type.toIntValue() == PropertyDataType.String.toIntValue()) {
			return value.getStringValue();
		} else if (type.toIntValue() == PropertyDataType.Text.toIntValue()) {
			return value.getStringValue();
		} else if (type.toIntValue() == PropertyDataType.PropertySet.toIntValue()) {
			return new PropertySetBuilder().addProperties(convertProperties(value.getPropertysetValue()))
					.createPropertySet();
		} else if (type.toIntValue() == PropertyDataType.PropertySetList.toIntValue()) {
			List<PropertySet> propertySetList = new ArrayList<PropertySet>();
			List<SparkplugBProto.Payload.PropertySet> list = value.getPropertysetsValue().getPropertysetList();
			for (SparkplugBProto.Payload.PropertySet decodedPropSet : list) {
				propertySetList.add(
						new PropertySetBuilder().addProperties(convertProperties(decodedPropSet)).createPropertySet());
			}
			return propertySetList;
		} else if (type.toIntValue() == PropertyDataType.Unknown.toIntValue()) {
			throw new Exception("Failed to decode: Unknown PropertyDataType " + type);
		} else {
			throw new Exception("Failed to decode: Unknown PropertyDataType " + type);
		}
	}

	private Object getMetricValue(SparkplugBProto.Payload.Metric protoMetric, MetricDataTypeMap metricDataTypeMap,
			String prefix) throws Exception {
		// Check if the null flag has been set indicating that the value is null
		if (protoMetric.getIsNull()) {
			return null;
		}

		// Get the MetricDataType
		int metricType = protoMetric.getDatatype();
		if (metricType == 0) {
			if (metricDataTypeMap != null && !metricDataTypeMap.isEmpty()) {
				if (protoMetric.hasName()) {
					metricType = metricDataTypeMap
							.getMetricDataType(prefix != null ? prefix + protoMetric.getName() : protoMetric.getName())
							.toIntValue();
				} else if (protoMetric.hasAlias()) {
					metricType = metricDataTypeMap.getMetricDataType(protoMetric.getAlias()).toIntValue();
				} else {
					logger.error("Failed to decode the payload on metric: {}", protoMetric);
					return null;
				}
			} else {
				logger.error("Failed to decode the payload on metric datatype: {}", protoMetric);
				return null;
			}
		}

		logger.trace("For metricName={} and alias={} - handling metric type in decoder: {}", protoMetric.getName(),
				protoMetric.getAlias(), metricType);
		if (metricType == MetricDataType.Boolean.toIntValue()) {
			return protoMetric.getBooleanValue();
		} else if (metricType == MetricDataType.DateTime.toIntValue()) {
			return new Date(protoMetric.getLongValue());
		} else if (metricType == MetricDataType.File.toIntValue()) {
			String filename = protoMetric.getMetadata().getFileName();
			byte[] fileBytes = protoMetric.getBytesValue().toByteArray();
			return new File(filename, fileBytes);
		} else if (metricType == MetricDataType.Float.toIntValue()) {
			return protoMetric.getFloatValue();
		} else if (metricType == MetricDataType.Double.toIntValue()) {
			return protoMetric.getDoubleValue();
		} else if (metricType == MetricDataType.Int8.toIntValue()) {
			return (byte) protoMetric.getIntValue();
		} else if (metricType == MetricDataType.Int16.toIntValue()) {
			return (short) protoMetric.getIntValue();
		} else if (metricType == MetricDataType.Int32.toIntValue()) {
			return protoMetric.getIntValue();
		} else if (metricType == MetricDataType.Int64.toIntValue()) {
			return protoMetric.getLongValue();
		} else if (metricType == MetricDataType.UInt8.toIntValue()) {
			return (short) protoMetric.getIntValue();
		} else if (metricType == MetricDataType.UInt16.toIntValue()) {
			return protoMetric.getIntValue();
		} else if (metricType == MetricDataType.UInt32.toIntValue()) {
			if (protoMetric.hasIntValue()) {
				return Integer.toUnsignedLong(protoMetric.getIntValue());
			} else if (protoMetric.hasLongValue()) {
				return protoMetric.getLongValue();
			} else {
				logger.error("Invalid value for UInt32 datatype");
				throw new Exception("Failed to decode: UInt32 MetricDataType " + metricType);
			}
		} else if (metricType == MetricDataType.UInt64.toIntValue()) {
			return new BigInteger(Long.toUnsignedString(protoMetric.getLongValue()));
		} else if (metricType == MetricDataType.String.toIntValue() || metricType == MetricDataType.Text.toIntValue()
				|| metricType == MetricDataType.UUID.toIntValue()) {
			return protoMetric.getStringValue();
		} else if (metricType == MetricDataType.Bytes.toIntValue()) {
			return protoMetric.getBytesValue().toByteArray();
		} else if (metricType == MetricDataType.DataSet.toIntValue()) {
			SparkplugBProto.Payload.DataSet protoDataSet = protoMetric.getDatasetValue();
			// Build the and create the DataSet
			return new DataSetBuilder(protoDataSet.getNumOfColumns()).addColumnNames(protoDataSet.getColumnsList())
					.addTypes(convertDataSetDataTypes(protoDataSet.getTypesList()))
					.addRows(convertDataSetRows(protoDataSet.getRowsList(), protoDataSet.getTypesList()))
					.createDataSet();
		} else if (metricType == MetricDataType.Template.toIntValue()) {
			SparkplugBProto.Payload.Template protoTemplate = protoMetric.getTemplateValue();
			List<Metric> metrics = new ArrayList<Metric>();
			List<Parameter> parameters = new ArrayList<Parameter>();

			for (SparkplugBProto.Payload.Template.Parameter protoParameter : protoTemplate.getParametersList()) {
				String name = protoParameter.getName();
				ParameterDataType type = ParameterDataType.fromInteger(protoParameter.getType());
				Object value = getParameterValue(protoParameter);
				if (logger.isTraceEnabled()) {
					logger.trace("Setting template parameter name: " + name + ", type: " + type + ", value: " + value
							+ ", valueType" + value.getClass());
				}

				parameters.add(new Parameter(name, type, value));
			}

			for (SparkplugBProto.Payload.Metric protoTemplateMetric : protoTemplate.getMetricsList()) {
				Metric templateMetric = convertMetric(protoTemplateMetric, metricDataTypeMap,
						prefix != null ? prefix + protoMetric.getName() + "/" : protoMetric.getName() + "/");
				if (logger.isTraceEnabled()) {
					logger.trace("Setting template parameter name: " + templateMetric.getName() + ", type: "
							+ templateMetric.getDataType() + ", value: " + templateMetric.getValue());
				}
				metrics.add(templateMetric);
			}

			Template template = new TemplateBuilder().version(protoTemplate.getVersion())
					.templateRef(protoTemplate.getTemplateRef()).definition(protoTemplate.getIsDefinition())
					.addMetrics(metrics).addParameters(parameters).createTemplate();

			if (logger.isTraceEnabled()) {
				logger.trace("Setting template - name: " + protoMetric.getName() + ", version: " + template.getVersion()
						+ ", ref: " + template.getTemplateRef() + ", isDef: " + template.isDefinition() + ", metrics: "
						+ metrics.size() + ", params: " + parameters.size());
			}

			return template;
		} else if (metricType == MetricDataType.Int8Array.toIntValue()) {
			ByteBuffer int8ByteBuffer = ByteBuffer.wrap(protoMetric.getBytesValue().toByteArray());
			List<Byte> int8List = new ArrayList<>();
			int8ByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
			while (int8ByteBuffer.hasRemaining()) {
				byte value = int8ByteBuffer.get();
				int8List.add(value);
			}
			return int8List.toArray(new Byte[0]);
		} else if (metricType == MetricDataType.Int16Array.toIntValue()) {
			ByteBuffer int16ByteBuffer = ByteBuffer.wrap(protoMetric.getBytesValue().toByteArray());
			List<Short> int16List = new ArrayList<>();
			int16ByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
			while (int16ByteBuffer.hasRemaining()) {
				short value = int16ByteBuffer.getShort();
				int16List.add(value);
			}
			return int16List.toArray(new Short[0]);
		} else if (metricType == MetricDataType.Int32Array.toIntValue()) {
			ByteBuffer int32ByteBuffer = ByteBuffer.wrap(protoMetric.getBytesValue().toByteArray());
			List<Integer> int32List = new ArrayList<>();
			int32ByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
			while (int32ByteBuffer.hasRemaining()) {
				int value = int32ByteBuffer.getInt();
				int32List.add(value);
			}
			return int32List.toArray(new Integer[0]);
		} else if (metricType == MetricDataType.Int64Array.toIntValue()) {
			ByteBuffer int64ByteBuffer = ByteBuffer.wrap(protoMetric.getBytesValue().toByteArray());
			List<Long> int64List = new ArrayList<>();
			int64ByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
			while (int64ByteBuffer.hasRemaining()) {
				long value = int64ByteBuffer.getLong();
				int64List.add(value);
			}
			return int64List.toArray(new Long[0]);
		} else if (metricType == MetricDataType.UInt8Array.toIntValue()) {
			ByteBuffer uInt8ByteBuffer = ByteBuffer.wrap(protoMetric.getBytesValue().toByteArray());
			List<Short> uInt8List = new ArrayList<>();
			uInt8ByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
			while (uInt8ByteBuffer.hasRemaining()) {
				byte value = uInt8ByteBuffer.get();
				uInt8List.add(value >= 0 ? (short) value : (short) (0x10000 + value));
			}
			return uInt8List.toArray(new Short[0]);
		} else if (metricType == MetricDataType.UInt16Array.toIntValue()) {
			ByteBuffer uInt16ByteBuffer = ByteBuffer.wrap(protoMetric.getBytesValue().toByteArray());
			List<Integer> uInt16List = new ArrayList<>();
			uInt16ByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
			while (uInt16ByteBuffer.hasRemaining()) {
				short value = uInt16ByteBuffer.getShort();
				uInt16List.add(Short.toUnsignedInt(value));
			}
			return uInt16List.toArray(new Integer[0]);
		} else if (metricType == MetricDataType.UInt32Array.toIntValue()) {
			ByteBuffer uInt32ByteBuffer = ByteBuffer.wrap(protoMetric.getBytesValue().toByteArray());
			List<Long> uInt32List = new ArrayList<>();
			uInt32ByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
			while (uInt32ByteBuffer.hasRemaining()) {
				int value = uInt32ByteBuffer.getInt();
				uInt32List.add(Integer.toUnsignedLong(value));
			}
			return uInt32List.toArray(new Long[0]);
		} else if (metricType == MetricDataType.UInt64Array.toIntValue()) {
			ByteBuffer uInt64ByteBuffer = ByteBuffer.wrap(protoMetric.getBytesValue().toByteArray());
			List<BigInteger> uInt64List = new ArrayList<>();
			uInt64ByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
			while (uInt64ByteBuffer.hasRemaining()) {
				long value = uInt64ByteBuffer.getLong();
				uInt64List.add(new BigInteger(Long.toUnsignedString(value)));
			}
			return uInt64List.toArray(new BigInteger[0]);
		} else if (metricType == MetricDataType.FloatArray.toIntValue()) {
			ByteBuffer floatByteBuffer = ByteBuffer.wrap(protoMetric.getBytesValue().toByteArray());
			List<Float> floatList = new ArrayList<>();
			floatByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
			while (floatByteBuffer.hasRemaining()) {
				float value = floatByteBuffer.getFloat();
				floatList.add(value);
			}
			return floatList.toArray(new Float[0]);
		} else if (metricType == MetricDataType.DoubleArray.toIntValue()) {
			ByteBuffer doubleByteBuffer = ByteBuffer.wrap(protoMetric.getBytesValue().toByteArray());
			List<Double> doubleList = new ArrayList<>();
			doubleByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
			while (doubleByteBuffer.hasRemaining()) {
				double value = doubleByteBuffer.getDouble();
				doubleList.add(value);
			}
			return doubleList.toArray(new Double[0]);
		} else if (metricType == MetricDataType.BooleanArray.toIntValue()) {
			ByteBuffer booleanByteBuffer = ByteBuffer.wrap(protoMetric.getBytesValue().toByteArray());
			List<Boolean> booleanList = new ArrayList<>();
			booleanByteBuffer.order(ByteOrder.LITTLE_ENDIAN);

			// The first 4 bytes is the number of boolean bytes
			int numberOfBooleans = booleanByteBuffer.getInt();
			int numberOfBytes = (int) Math.ceil((double) numberOfBooleans / 8);

			// Boolean[] booleanArray = new boolean[booleanBytes.length * 8];
			for (int i = 0; i < numberOfBytes; i++) {
				byte nextByte = booleanByteBuffer.get();
				for (int j = 0; j < 8; j++) {
					if (i * 8 + j < numberOfBooleans) {
						if ((nextByte & (1 << (7 - j))) > 0) {
							booleanList.add(true);
						} else {
							booleanList.add(false);
						}
					}
				}
			}
			return booleanList.toArray(new Boolean[0]);
		} else if (metricType == MetricDataType.StringArray.toIntValue()) {
			ByteBuffer stringByteBuffer = ByteBuffer.wrap(protoMetric.getBytesValue().toByteArray());
			List<String> stringList = new ArrayList<>();
			stringByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
			ByteBuffer subByteBuffer = ByteBuffer.allocate(protoMetric.getBytesValue().toByteArray().length);
			while (stringByteBuffer.hasRemaining()) {
				byte b = stringByteBuffer.get();
				if (b == (byte) 0) {
					String string = new String(subByteBuffer.array(), StandardCharsets.UTF_8);
					if (string != null && string.lastIndexOf("\0") == string.length() - 1) {
						string = string.replace("\0", "");
					}
					stringList.add(string);
					subByteBuffer = ByteBuffer.allocate(protoMetric.getBytesValue().toByteArray().length);
				} else {
					subByteBuffer.put(b);
				}
			}
			return stringList.toArray(new String[0]);
		} else if (metricType == MetricDataType.DateTimeArray.toIntValue()) {
			ByteBuffer dateTimeByteBuffer = ByteBuffer.wrap(protoMetric.getBytesValue().toByteArray());
			List<Date> dateTimeList = new ArrayList<>();
			dateTimeByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
			while (dateTimeByteBuffer.hasRemaining()) {
				long value = dateTimeByteBuffer.getLong();
				Date date = new Date(value);
				dateTimeList.add(date);
			}
			return dateTimeList.toArray(new Date[0]);
		} else {
			throw new Exception("Failed to decode: Unknown MetricDataType " + metricType);
		}
	}

	protected Collection<Row> convertDataSetRows(List<SparkplugBProto.Payload.DataSet.Row> protoRows,
			List<Integer> protoTypes) throws Exception {
		Collection<Row> rows = new ArrayList<Row>();
		if (protoRows != null) {
			for (SparkplugBProto.Payload.DataSet.Row protoRow : protoRows) {
				List<SparkplugBProto.Payload.DataSet.DataSetValue> protoValues = protoRow.getElementsList();
				List<Value<?>> values = new ArrayList<Value<?>>();
				for (int index = 0; index < protoRow.getElementsCount(); index++) {
					values.add(convertDataSetValue(protoTypes.get(index), protoValues.get(index)));
				}
				// Add the values to the row and the row to the rows
				rows.add(new RowBuilder().addValues(values).createRow());
			}
		}
		return rows;
	}

	protected Collection<DataSetDataType> convertDataSetDataTypes(List<Integer> protoTypes) {
		List<DataSetDataType> types = new ArrayList<DataSetDataType>();
		// Build up a List of column types
		for (int type : protoTypes) {
			types.add(DataSetDataType.fromInteger(type));
		}
		return types;
	}

	private Object getParameterValue(SparkplugBProto.Payload.Template.Parameter protoParameter) throws Exception {
		// Otherwise convert the value based on the type
		int type = protoParameter.getType();
		if (type == MetricDataType.Boolean.toIntValue()) {
			return protoParameter.getBooleanValue();
		} else if (type == MetricDataType.DateTime.toIntValue()) {
			return new Date(protoParameter.getLongValue());
		} else if (type == MetricDataType.Float.toIntValue()) {
			return protoParameter.getFloatValue();
		} else if (type == MetricDataType.Double.toIntValue()) {
			return protoParameter.getDoubleValue();
		} else if (type == MetricDataType.Int8.toIntValue()) {
			return (byte) protoParameter.getIntValue();
		} else if (type == MetricDataType.Int16.toIntValue()) {
			return (short) protoParameter.getIntValue();
		} else if (type == MetricDataType.Int32.toIntValue()) {
			return protoParameter.getIntValue();
		} else if (type == MetricDataType.Int64.toIntValue()) {
			return protoParameter.getLongValue();
		} else if (type == MetricDataType.UInt8.toIntValue()) {
			return (short) protoParameter.getIntValue();
		} else if (type == MetricDataType.UInt16.toIntValue()) {
			return protoParameter.getIntValue();
		} else if (type == MetricDataType.UInt32.toIntValue()) {
			if (protoParameter.hasIntValue()) {
				return Integer.toUnsignedLong(protoParameter.getIntValue());
			} else if (protoParameter.hasLongValue()) {
				return protoParameter.getLongValue();
			} else {
				logger.error("Invalid value for UInt32 datatype");
				throw new Exception("Failed to decode: UInt32 Parameter Type " + type);
			}
		} else if (type == MetricDataType.UInt64.toIntValue()) {
			return new BigInteger(Long.toUnsignedString(protoParameter.getLongValue()));
		} else if (type == MetricDataType.String.toIntValue() || type == MetricDataType.Text.toIntValue()) {
			return protoParameter.getStringValue();
		} else {
			throw new Exception("Failed to decode: Unknown Parameter Type " + type);
		}
	}

	protected Value<?> convertDataSetValue(int protoType, SparkplugBProto.Payload.DataSet.DataSetValue protoValue)
			throws Exception {
		DataSetDataType type = DataSetDataType.fromInteger(protoType);
		if (protoType == DataSetDataType.Boolean.toIntValue()) {
			if (protoValue.hasBooleanValue()) {
				return new Value<Boolean>(type, protoValue.getBooleanValue());
			} else {
				return new Value<Boolean>(type, null);
			}
		} else if (protoType == DataSetDataType.DateTime.toIntValue()) {
			if (protoValue.hasLongValue()) {
				if (protoValue.getLongValue() == -9223372036854775808L) {
					return new Value<Date>(type, null);
				} else {
					return new Value<Date>(type, new Date(protoValue.getLongValue()));
				}
			} else {
				return new Value<Date>(type, null);
			}
		} else if (protoType == DataSetDataType.Float.toIntValue()) {
			if (protoValue.hasFloatValue()) {
				return new Value<Float>(type, protoValue.getFloatValue());
			} else {
				return new Value<Float>(type, null);
			}
		} else if (protoType == DataSetDataType.Double.toIntValue()) {
			if (protoValue.hasDoubleValue()) {
				return new Value<Double>(type, protoValue.getDoubleValue());
			} else {
				return new Value<Double>(type, null);
			}
		} else if (protoType == DataSetDataType.Int8.toIntValue()) {
			if (protoValue.hasIntValue()) {
				return new Value<Byte>(type, (byte) protoValue.getIntValue());
			} else {
				return new Value<Byte>(type, null);
			}
		} else if (protoType == DataSetDataType.Int16.toIntValue()) {
			if (protoValue.hasIntValue()) {
				return new Value<Short>(type, (short) protoValue.getIntValue());
			} else {
				return new Value<Short>(type, null);
			}
		} else if (protoType == DataSetDataType.Int32.toIntValue()) {
			if (protoValue.hasIntValue()) {
				return new Value<Integer>(type, protoValue.getIntValue());
			} else {
				return new Value<Integer>(type, null);
			}
		} else if (protoType == DataSetDataType.Int64.toIntValue()) {
			if (protoValue.hasLongValue()) {
				return new Value<Long>(type, protoValue.getLongValue());
			} else {
				return new Value<Long>(type, null);
			}
		} else if (protoType == DataSetDataType.UInt8.toIntValue()) {
			if (protoValue.hasIntValue()) {
				return new Value<Short>(type, (short) protoValue.getIntValue());
			} else {
				return new Value<Short>(type, null);
			}
		} else if (protoType == DataSetDataType.UInt16.toIntValue()) {
			if (protoValue.hasIntValue()) {
				return new Value<Integer>(type, protoValue.getIntValue());
			} else {
				return new Value<Integer>(type, null);
			}
		} else if (protoType == DataSetDataType.UInt32.toIntValue()) {
			if (protoValue.hasIntValue()) {
				return new Value<Long>(type, Integer.toUnsignedLong(protoValue.getIntValue()));
			} else if (protoValue.hasLongValue()) {
				return new Value<Long>(type, protoValue.getLongValue());
			} else {
				return new Value<Long>(type, null);
			}
		} else if (protoType == DataSetDataType.UInt64.toIntValue()) {
			if (protoValue.hasLongValue()) {
				return new Value<BigInteger>(type, new BigInteger(Long.toUnsignedString(protoValue.getLongValue())));
			} else {
				return new Value<BigInteger>(type, null);
			}
		} else if (protoType == DataSetDataType.String.toIntValue() || protoType == DataSetDataType.Text.toIntValue()) {
			if (protoValue.hasStringValue()) {
				if (protoValue.getStringValue().equals("null")) {
					return new Value<String>(type, null);
				} else {
					return new Value<String>(type, protoValue.getStringValue());
				}
			} else {
				return new Value<String>(type, null);
			}
		} else {
			logger.error("Unknown DataSetDataType: " + protoType);
			throw new Exception("Failed to decode");
		}
	}
}
