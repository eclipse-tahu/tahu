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

package org.eclipse.tahu.message.model;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Date;

import org.eclipse.tahu.SparkplugException;
import org.eclipse.tahu.SparkplugInvalidTypeException;
import org.eclipse.tahu.exception.TahuErrorCode;
import org.eclipse.tahu.exception.TahuException;
import org.eclipse.tahu.message.model.DataSet.DataSetBuilder;
import org.eclipse.tahu.message.model.MetaData.MetaDataBuilder;
import org.eclipse.tahu.message.model.PropertySet.PropertySetBuilder;
import org.eclipse.tahu.message.model.Template.TemplateBuilder;
import org.eclipse.tahu.protobuf.SparkplugBProto.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

/**
 * A metric of a Sparkplug Payload.
 */
@JsonIgnoreProperties(
		value = { "isNull" })
@JsonInclude(Include.NON_NULL)
public class Metric {

	private static Logger logger = LoggerFactory.getLogger(Metric.class.getName());

	@JsonProperty("name")
	private String name;

	@JsonProperty("alias")
	private Long alias;

	@JsonProperty("timestamp")
	private Date timestamp;

	@JsonProperty("dataType")
	private MetricDataType dataType;

	@JsonProperty("isHistorical")
	private Boolean isHistorical;

	@JsonProperty("isTransient")
	private Boolean isTransient;

	@JsonProperty("metaData")
	@JsonInclude(Include.NON_EMPTY)
	private MetaData metaData;

	@JsonProperty("properties")
	@JsonInclude(Include.NON_EMPTY)
	private PropertySet properties;

	@JsonProperty("value")
	@JsonInclude(Include.ALWAYS)
	private Object value;

	private Boolean isNull = null;

	/**
	 * Default Constructor
	 */
	public Metric() {
	};

	/**
	 * Constructor
	 *
	 * @param name the name of the {@link Metric}
	 * @param alias the alias of the {@link Metric}
	 * @param timestamp the timestamp of the {@link Metric} representing the time at which the {@link Metric} changed in
	 *            UDT time
	 * @param dataType the {@link MetricDataType} of the {@link Metric}
	 * @param isHistorical whether or not this {@link Metric} is a historical value
	 * @param isTransient whether or not this {@link Metric} is a transient value
	 * @param metaData the {@link MetaData} assocated with this {@link Metric}
	 * @param properties the {@link PropertySet} associated with this {@link Metric}
	 * @param value the {@link Object} value of this {@link Metric} that must be a {@link Object} type for the
	 *            {@link MetricDataType}
	 *
	 * @throws SparkplugInvalidTypeException if the value is not a valid {@link Object} type for the supplied
	 *             {@link MetricDataType}
	 */
	public Metric(String name, Long alias, Date timestamp, MetricDataType dataType, Boolean isHistorical,
			Boolean isTransient, MetaData metaData, PropertySet properties, Object value)
			throws SparkplugInvalidTypeException {
		super();
		this.name = name;
		this.alias = alias;
		this.timestamp = timestamp;
		this.dataType = dataType;
		this.isHistorical = isHistorical;
		this.isTransient = isTransient;
		isNull = (value == null) ? true : false;
		this.metaData = metaData;
		this.properties = properties;
		this.value = value;
		this.dataType.checkType(value);
	}

	/**
	 * Copy Constructor
	 *
	 * @param metric the {@link Metric} to copy
	 * @throws SparkplugInvalidTypeException if the {@link Metric} can not be copied due to an invalid {@link DataType}
	 */
	public Metric(Metric metric) throws Exception {
		if (metric != null) {
			this.name = metric.getName();
			this.alias = metric.getAlias();
			this.timestamp = metric.getTimestamp();
			this.dataType = metric.getDataType();
			this.isHistorical = metric.getIsHistorical();
			this.isTransient = metric.getIsTransient();
			this.metaData = metric.getMetaData() != null ? new MetaData(metric.getMetaData()) : null;
			this.properties = metric.getProperties() != null ? new PropertySet(metric.getProperties()) : null;

			if (metric.getValue() != null) {
				if (metric.getDataType() == MetricDataType.DataSet) {
					this.value = new DataSet((DataSet) metric.getValue());
				} else if (metric.getDataType() == MetricDataType.Bytes) {
					byte[] originalValue = (byte[]) metric.getValue();
					this.value = Arrays.copyOf(originalValue, originalValue.length);
				} else if (metric.getDataType() == MetricDataType.File) {
					this.value = new File((File) metric.getValue());
				} else if (metric.getDataType() == MetricDataType.Int8Array) {
					Byte[] originalValue = (Byte[]) metric.getValue();
					this.value = Arrays.copyOf(originalValue, originalValue.length);
				} else if (metric.getDataType() == MetricDataType.Int16Array) {
					Short[] originalValue = (Short[]) metric.getValue();
					this.value = Arrays.copyOf(originalValue, originalValue.length);
				} else if (metric.getDataType() == MetricDataType.Int32Array) {
					Integer[] originalValue = (Integer[]) metric.getValue();
					this.value = Arrays.copyOf(originalValue, originalValue.length);
				} else if (metric.getDataType() == MetricDataType.Int64Array) {
					Long[] originalValue = (Long[]) metric.getValue();
					this.value = Arrays.copyOf(originalValue, originalValue.length);
				} else if (metric.getDataType() == MetricDataType.UInt8Array) {
					Short[] originalValue = (Short[]) metric.getValue();
					this.value = Arrays.copyOf(originalValue, originalValue.length);
				} else if (metric.getDataType() == MetricDataType.UInt16Array) {
					Integer[] originalValue = (Integer[]) metric.getValue();
					this.value = Arrays.copyOf(originalValue, originalValue.length);
				} else if (metric.getDataType() == MetricDataType.UInt32Array) {
					Long[] originalValue = (Long[]) metric.getValue();
					this.value = Arrays.copyOf(originalValue, originalValue.length);
				} else if (metric.getDataType() == MetricDataType.UInt64Array) {
					BigInteger[] originalValue = (BigInteger[]) metric.getValue();
					this.value = Arrays.copyOf(originalValue, originalValue.length);
				} else if (metric.getDataType() == MetricDataType.FloatArray) {
					Float[] originalValue = (Float[]) metric.getValue();
					this.value = Arrays.copyOf(originalValue, originalValue.length);
				} else if (metric.getDataType() == MetricDataType.DoubleArray) {
					Double[] originalValue = (Double[]) metric.getValue();
					this.value = Arrays.copyOf(originalValue, originalValue.length);
				} else if (metric.getDataType() == MetricDataType.BooleanArray) {
					Boolean[] originalValue = (Boolean[]) metric.getValue();
					this.value = Arrays.copyOf(originalValue, originalValue.length);
				} else if (metric.getDataType() == MetricDataType.StringArray) {
					String[] originalValue = (String[]) metric.getValue();
					this.value = Arrays.copyOf(originalValue, originalValue.length);
				} else if (metric.getDataType() == MetricDataType.DateTimeArray) {
					Date[] originalValue = (Date[]) metric.getValue();
					this.value = Arrays.copyOf(originalValue, originalValue.length);
				} else if (metric.getDataType() == MetricDataType.Template) {
					this.value = new Template((Template) metric.getValue());
				} else {
					this.value = metric.getValue();
				}
			}

			// Check the value is valid per the datatype
			this.dataType.checkType(value);
		}
	}

	/**
	 * Gets the name of the {@link Metric}
	 *
	 * @return the name of the {@link Metric}
	 */
	public String getName() {
		return name;
	}

	/**
	 * Sets the name of the {@link Metric}
	 *
	 * @param name the name of the {@link Metric}
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Whether or not this {@link Metric} has a name
	 *
	 * @return true if the name is not null, otherwise false
	 */
	public boolean hasName() {
		return !(name == null);
	}

	@JsonIgnore
	public String getKey() throws Exception {
		String primaryKeyComponent = hasName() ? getName() : hasAlias() ? getAlias().toString() : null;

		if (dataType == MetricDataType.Template) {
			String key = getTimestamp().getTime() + "_" + getSecondaryKeyComponent(primaryKeyComponent, this);
			logger.debug("Returning template key: {}", key);
			return key;
		} else {
			// Fall through to standard metrics
			if (primaryKeyComponent != null) {
				String key = getTimestamp().getTime() + "_" + primaryKeyComponent;
				logger.debug("Returning regular key: {}", key);
				return key;
			} else {
				return null;
			}
		}
	}

	private String getSecondaryKeyComponent(String prefix, Metric metric) throws Exception {
		if (dataType == MetricDataType.Template) {
			if (metric.getValue() != null) {
				if (Template.class.isAssignableFrom(metric.getValue().getClass())) {
					Template template = (Template) metric.getValue();
					if (template.getMetrics() != null && template.getMetrics().size() == 1) {
						Metric memberMetric = template.getMetrics().get(0);
						String secondaryName = memberMetric.hasName()
								? memberMetric.getName()
								: memberMetric.hasAlias() ? memberMetric.getAlias().toString() : null;
						if (secondaryName != null) {
							if (memberMetric.getDataType() == MetricDataType.Template) {
								return getSecondaryKeyComponent(prefix + "_" + secondaryName, memberMetric);
							} else {
								return prefix + "_" + secondaryName;
							}
						} else {
							throw new TahuException(TahuErrorCode.INVALID_ARGUMENT,
									"Template Metric has an invalid config " + metric);
						}
					} else {
						throw new TahuException(TahuErrorCode.INVALID_ARGUMENT,
								"Template Metrics must only have a single member Metric " + metric);
					}
				} else if (TemplateMap.class.isAssignableFrom(metric.getValue().getClass())) {
					TemplateMap templateMap = (TemplateMap) metric.getValue();
					if (templateMap.getMetrics() != null && templateMap.getMetrics().size() == 1) {
						Metric memberMetric = templateMap.getMetrics().get(0);
						String secondaryName = memberMetric.hasName()
								? memberMetric.getName()
								: memberMetric.hasAlias() ? memberMetric.getAlias().toString() : null;
						if (secondaryName != null) {
							if (memberMetric.getDataType() == MetricDataType.Template) {
								return getSecondaryKeyComponent(prefix + "_" + secondaryName, memberMetric);
							} else {
								return prefix + "_" + secondaryName;
							}
						} else {
							throw new TahuException(TahuErrorCode.INVALID_ARGUMENT,
									"Template Metric has an invalid config " + metric);
						}
					} else {
						throw new TahuException(TahuErrorCode.INVALID_ARGUMENT,
								"TemplateMap Metrics must only have a single member Metric " + metric);
					}
				} else {
					throw new TahuException(TahuErrorCode.INVALID_ARGUMENT,
							"Invalid Template MetricDataType " + metric);
				}
			} else {
				throw new TahuException(TahuErrorCode.INVALID_ARGUMENT,
						"Template Metrics must only have one member Metric " + metric);
			}
		} else {
			return prefix;
		}
	}

	/**
	 * Whether or not this {@link Metric} has an alias
	 *
	 * @return true if the {@link Metric} has an alias, otherwise false
	 */
	public boolean hasAlias() {
		return !(alias == null);
	}

	/**
	 * Gets the alias associated with the {@link Metric}
	 *
	 * @return the alias associated with the {@link Metric}
	 */
	public Long getAlias() {
		return alias;
	}

	/**
	 * Sets the alias for the {@link Metric}
	 *
	 * @param alias the alias to set for the {@link Metric}
	 */
	public void setAlias(long alias) {
		this.alias = alias;
	}

	/**
	 * Gets the timestamp associated with the {@link Metric}
	 *
	 * @return the timestamp associated with the {@link Metric}
	 */
	public Date getTimestamp() {
		return timestamp;
	}

	/**
	 * Sets the timestamp associated with the {@link Metric}
	 *
	 * @param timestamp the timestamp associated with the {@link Metric}
	 */
	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	/**
	 * Gets the {@link MetricDataType} associated with the {@link Metric}
	 *
	 * @return the {@link MetricDataType} associated with the {@link Metric}
	 */
	public MetricDataType getDataType() {
		return dataType;
	}

	/**
	 * Sets the {@link MetricDataType} associated with the {@link Metric}
	 *
	 * @param dataType the {@link MetricDataType} associated with the {@link Metric}
	 */
	public void setDataType(MetricDataType dataType) {
		this.dataType = dataType;
	}

	/**
	 * Gets the {@link MetaData} associated with the {@link Metric}
	 *
	 * @return the {@link MetaData} associated with the {@link Metric}
	 */
	@JsonGetter("metaData")
	public MetaData getMetaData() {
		return metaData;
	}

	/**
	 * Sets the {@link MetaData} associated with the {@link Metric}
	 *
	 * @param metadata the {@link MetaData} associated with the {@link Metric}
	 */
	@JsonSetter("metaData")
	public void setMetaData(MetaData metaData) {
		this.metaData = metaData;
	}

	/**
	 * Gets the {@link Object} value associated with the {@link Metric}
	 *
	 * @return the {@link Object} value associated with the {@link Metric}
	 */
	public Object getValue() {
		return value;
	}

	/**
	 * Sets the {@link Object} value associated with the {@link Metric}
	 *
	 * @param value the {@link Object} value associated with the {@link Metric}
	 */
	public void setValue(Object value) {
		this.value = value;
		isNull = (value == null);
	}

	/**
	 * Gets the {@link PropertySet} associated with the {@link Metric}
	 *
	 * @return the {@link PropertySet} associated with the {@link Metric}
	 */
	public PropertySet getProperties() {
		return this.properties;
	}

	/**
	 * Sets the {@link PropertySet} associated with the {@link Metric}
	 *
	 * @param metadata the {@link PropertySet} associated with the {@link Metric}
	 */
	public void setProperties(PropertySet properties) {
		this.properties = properties;
	}

	/**
	 * Whether or not this {@link Metric} is historical
	 *
	 * @return true if this is a historical {@link Metric}, otherwise false
	 */
	@JsonIgnore
	public Boolean isHistorical() {
		return isHistorical == null ? false : isHistorical;
	}

	/**
	 * Whether or not this {@link Metric} is historical
	 *
	 * @return true if this is a historical {@link Metric}, otherwise false
	 */
	@JsonGetter("isHistorical")
	public Boolean getIsHistorical() {
		return isHistorical;
	}

	/**
	 * Sets the historical flag for this {@link Metric}
	 *
	 * @param isHistorical true if this is a historical {@link Metric}, otherwise false
	 */
	@JsonSetter("isHistorical")
	public void setHistorical(Boolean isHistorical) {
		this.isHistorical = isHistorical;
	}

	/**
	 * Whether or not this {@link Metric} is transient
	 *
	 * @return true if this is a transient {@link Metric}, otherwise false
	 */
	@JsonIgnore
	public Boolean isTransient() {
		return isTransient == null ? false : isTransient;
	}

	/**
	 * Whether or not this {@link Metric} is transient
	 *
	 * @return true if this is a transient {@link Metric}, otherwise false
	 */
	@JsonGetter("isTransient")
	public Boolean getIsTransient() {
		return isTransient;
	}

	/**
	 * Sets the transient flag for this {@link Metric}
	 *
	 * @param transient true if this is a transient {@link Metric}, otherwise false
	 */
	@JsonSetter("isTransient")
	public void setTransient(Boolean isTransient) {
		this.isTransient = isTransient;
	}

	/**
	 * Return true if this value is null, otherwise false
	 *
	 * @return true if this value is null, otherwise false
	 */
	@JsonIgnore
	public Boolean isNull() {
		return isNull == null ? false : isNull;
	}

	/**
	 * Return true if this value is null, otherwise false
	 *
	 * @return true if this value is null, otherwise false
	 */
	@JsonIgnore
	public Boolean getIsNull() {
		return isNull;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((alias == null) ? 0 : alias.hashCode());
		result = prime * result + ((dataType == null) ? 0 : dataType.hashCode());
		result = prime * result + ((isHistorical == null) ? 0 : isHistorical.hashCode());
		result = prime * result + ((isNull == null) ? 0 : isNull.hashCode());
		result = prime * result + ((isTransient == null) ? 0 : isTransient.hashCode());
		result = prime * result + ((metaData == null) ? 0 : metaData.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((properties == null) ? 0 : properties.hashCode());
		result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Metric other = (Metric) obj;
		if (alias == null) {
			if (other.alias != null)
				return false;
		} else if (!alias.equals(other.alias))
			return false;
		if (dataType != other.dataType)
			return false;
		if (isHistorical == null) {
			if (other.isHistorical != null)
				return false;
		} else if (!isHistorical.equals(other.isHistorical))
			return false;
		if (isNull == null) {
			if (other.isNull != null)
				return false;
		} else if (!isNull.equals(other.isNull))
			return false;
		if (isTransient == null) {
			if (other.isTransient != null)
				return false;
		} else if (!isTransient.equals(other.isTransient))
			return false;
		if (metaData == null) {
			if (other.metaData != null)
				return false;
		} else if (!metaData.equals(other.metaData))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (properties == null) {
			if (other.properties != null)
				return false;
		} else if (!properties.equals(other.properties))
			return false;
		if (timestamp == null) {
			if (other.timestamp != null)
				return false;
		} else if (!timestamp.equals(other.timestamp))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Metric [name=");
		builder.append(name);
		builder.append(", alias=");
		builder.append(alias);
		builder.append(", timestamp=");
		builder.append(timestamp != null ? timestamp.getTime() : "null");
		builder.append(", dataType=");
		builder.append(dataType);
		builder.append(", isHistorical=");
		builder.append(isHistorical);
		builder.append(", isTransient=");
		builder.append(isTransient);
		builder.append(", metaData=");
		builder.append(metaData);
		builder.append(", properties=");
		builder.append(properties);
		builder.append(", value=");
		if (dataType == MetricDataType.BooleanArray) {
			builder.append(Arrays.toString((Boolean[]) value));
		} else if (dataType == MetricDataType.DateTimeArray) {
			builder.append(Arrays.toString((Date[]) value));
		} else if (dataType == MetricDataType.DoubleArray) {
			builder.append(Arrays.toString((Double[]) value));
		} else if (dataType == MetricDataType.FloatArray) {
			builder.append(Arrays.toString((Float[]) value));
		} else if (dataType == MetricDataType.Int8Array) {
			builder.append(Arrays.toString((Byte[]) value));
		} else if (dataType == MetricDataType.Int16Array) {
			builder.append(Arrays.toString((Short[]) value));
		} else if (dataType == MetricDataType.Int32Array) {
			builder.append(Arrays.toString((Integer[]) value));
		} else if (dataType == MetricDataType.Int64Array) {
			builder.append(Arrays.toString((Long[]) value));
		} else if (dataType == MetricDataType.StringArray) {
			builder.append(Arrays.toString((String[]) value));
		} else if (dataType == MetricDataType.UInt8Array) {
			builder.append(Arrays.toString((Short[]) value));
		} else if (dataType == MetricDataType.UInt16Array) {
			builder.append(Arrays.toString((Integer[]) value));
		} else if (dataType == MetricDataType.UInt32Array) {
			builder.append(Arrays.toString((Long[]) value));
		} else if (dataType == MetricDataType.UInt64Array) {
			builder.append(Arrays.toString((BigInteger[]) value));
		} else {
			builder.append(value);
		}
		builder.append(", isNull=");
		builder.append(isNull);
		builder.append("]");
		return builder.toString();
	}

	/**
	 * A builder for creating a {@link Metric} instance.
	 */
	public static class MetricBuilder {

		private String name;
		private Long alias;
		private Date timestamp;
		private MetricDataType dataType;
		private Boolean isHistorical;
		private Boolean isTransient;
		private MetaData metaData = null;
		private PropertySet properties = null;
		private Object value;

		public MetricBuilder(String name, MetricDataType dataType, Object value) {
			this.name = name;
			this.timestamp = new Date();
			this.dataType = dataType;
			this.value = value;
		}

		public MetricBuilder(Long alias, MetricDataType dataType, Object value) {
			this.alias = alias;
			this.timestamp = new Date();
			this.dataType = dataType;
			this.value = value;
		}

		public MetricBuilder(Metric metric) throws SparkplugException {
			this.name = metric.getName();
			this.alias = metric.getAlias();
			this.timestamp = metric.getTimestamp();
			this.dataType = metric.getDataType();
			this.isHistorical = metric.isHistorical();
			this.isTransient = metric.isTransient();
			this.metaData =
					metric.getMetaData() != null ? new MetaDataBuilder(metric.getMetaData()).createMetaData() : null;
			this.properties = metric.getMetaData() != null
					? new PropertySetBuilder(metric.getProperties()).createPropertySet()
					: null;

			if (dataType.toIntValue() == MetricDataType.DataSet.toIntValue()) {
				this.value = metric.getValue() != null
						? new DataSetBuilder((DataSet) metric.getValue()).createDataSet()
						: null;
			} else if (dataType.toIntValue() == MetricDataType.Template.toIntValue()) {
				this.value = metric.getValue() != null
						? new TemplateBuilder((Template) metric.getValue()).createTemplate()
						: null;
			} else {
				this.value = metric.getValue();
			}
		}

		public MetricBuilder name(String name) {
			this.name = name;
			return this;
		}

		public MetricBuilder alias(Long alias) {
			this.alias = alias;
			return this;
		}

		public MetricBuilder timestamp(Date timestamp) {
			this.timestamp = timestamp;
			return this;
		}

		public MetricBuilder dataType(MetricDataType dataType) {
			this.dataType = dataType;
			return this;
		}

		public MetricBuilder isHistorical(Boolean isHistorical) {
			this.isHistorical = isHistorical;
			return this;
		}

		public MetricBuilder isTransient(Boolean isTransient) {
			this.isTransient = isTransient;
			return this;
		}

		public MetricBuilder metaData(MetaData metaData) {
			this.metaData = metaData;
			return this;
		}

		public MetricBuilder properties(PropertySet properties) {
			this.properties = properties;
			return this;
		}

		public MetricBuilder value(Object value) {
			this.value = value;
			return this;
		}

		public Metric createMetric() throws SparkplugInvalidTypeException {
			return new Metric(name, alias, timestamp, dataType, isHistorical, isTransient, metaData, properties, value);
		}
	}
}
