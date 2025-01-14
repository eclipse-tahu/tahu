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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import org.eclipse.tahu.SparkplugInvalidTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * The value of a property in a {@link PropertySet}.
 */
public class PropertyValue<T extends PropertyDataType> {

	private static Logger logger = LoggerFactory.getLogger(PropertyValue.class.getName());

	private T type;
	private Object value;
	private Boolean isNull = null;

	/**
	 * Default Constructor
	 */
	public PropertyValue() {
	}

	/**
	 * Constructor.
	 * 
	 * @param type the property type
	 * @param value the property value
	 * @throws SparkplugInvalidTypeException
	 */
	public PropertyValue(T type, Object value) throws SparkplugInvalidTypeException {
		this.type = type;
		this.value = value;
		isNull = (value == null) ? true : false;
		type.checkType(value);
	}

	public PropertyValue(PropertyValue<T> propertyValue) throws Exception {
		this.type = propertyValue.getType();
		this.isNull = propertyValue.isNull();

		if (!isNull) {
			if (type == PropertyDataType.DateTime) {
				this.value = new Date(((Date) value).getTime());
			} else if (type == PropertyDataType.PropertySet) {
				this.value = new PropertySet((PropertySet) value);
			} else if (type == PropertyDataType.PropertySetList) {
				@SuppressWarnings("unchecked")
				List<PropertySet> originalPropertySetList = (List<PropertySet>) value;
				List<PropertySet> newPropertySetList = new ArrayList<PropertySet>();
				for (PropertySet propertySet : originalPropertySetList) {
					newPropertySetList.add(new PropertySet(propertySet));
				}
				this.value = newPropertySetList;
			} else {
				this.value = propertyValue.getValue();
			}
		}
		type.checkType(value);

		logger.trace("Copying - Orig: {}", propertyValue);
		logger.trace("copying - New : {}", this);
	}

	/**
	 * Gets the {@link PropertyDataType} of the {@link PropertyValue}
	 *
	 * @return the {@link PropertyDataType} of the {@link PropertyValue}
	 */
	public T getType() {
		return type;
	}

	/**
	 * Sets the {@link PropertyDataType} of this {@link PropertyValue}
	 *
	 * @param type the {@link PropertyDataType} of this {@link PropertyValue}
	 */
	public void setType(T type) {
		this.type = type;
	}

	/**
	 * Gets the {@link Object} value of the {@link PropertyValue}
	 *
	 * @return the {@link Object} value of the {@link PropertyValue}
	 */
	public Object getValue() {
		return value;
	}

	/**
	 * Sets the {@link Object} value of this {@link PropertyValue}
	 *
	 * @param type the {@link Object} value of this {@link PropertyValue}
	 */
	public void setValue(Object value) {
		this.value = value;
		isNull = (value == null) ? true : false;
	}

	/**
	 * Whether or not this {@link PropertyValue} is null or not
	 *
	 * @return true if this {@link PropertyValue} is null, otherwise false
	 */
	@JsonIgnore
	public Boolean isNull() {
		return isNull;
	}

	@Override
	public boolean equals(Object object) {
		if (this == object) {
			return true;
		}
		if (object == null || this.getClass() != object.getClass()) {
			return false;
		}
		@SuppressWarnings("unchecked")
		PropertyValue<T> propValue = (PropertyValue<T>) object;
		return Objects.equals(type, propValue.getType()) && Objects.equals(value, propValue.getValue());
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("PropertyValue [type=");
		builder.append(type);
		builder.append(", value=");
		builder.append(value);
		builder.append(", isNull=");
		builder.append(isNull);
		builder.append("]");
		return builder.toString();
	}
}
