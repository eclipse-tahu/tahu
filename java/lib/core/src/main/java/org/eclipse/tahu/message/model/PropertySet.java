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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.eclipse.tahu.SparkplugInvalidTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * A class that maintains a set of properties associated with a {@link Metric}.
 */
public class PropertySet implements Map<String, PropertyValue<? extends PropertyDataType>> {

	private static Logger logger = LoggerFactory.getLogger(PropertySet.class.getName());

	@JsonIgnore
	private Map<String, PropertyValue<? extends PropertyDataType>> map;

	/**
	 * Default Constructor
	 */
	public PropertySet() {
		this.map = new HashMap<>();
	}

	/**
	 * Copy Constructor
	 *
	 * @param propertySet the {@link PropertySet} to copy
	 * @throws Exception
	 */
	public PropertySet(PropertySet propertySet) throws Exception {
		this.map = new HashMap<>();
		if (propertySet.getPropertyMap() != null) {
			for (Entry<String, PropertyValue<? extends PropertyDataType>> entry : propertySet.getPropertyMap()
					.entrySet()) {
				this.map.put(entry.getKey(), new PropertyValue<>(entry.getValue()));
			}
		}

		logger.trace("Copying - Orig: {}", propertySet);
		logger.trace("Copying - New : {}", this);
	}

	/**
	 * Constructor
	 *
	 * @param propertyMap the {@link Map} of {@link String}s to {@link PropertyValue}s
	 */
	private PropertySet(Map<String, PropertyValue<? extends PropertyDataType>> propertyMap) {
		this.map = propertyMap;
	}

	/**
	 * Gets the {@link PropertyValue} associated with a given Property name
	 *
	 * @param name the name of the Property
	 *
	 * @return the {@link PropertyValue} associated with the name
	 */
	@JsonIgnore
	public PropertyValue<? extends PropertyDataType> getPropertyValue(String name) {
		return this.map.get(name);
	}

	/**
	 * Sets the {@link PropertyValue} for a give property name
	 *
	 * @param name the name of the property
	 * @param value the {@link PropertyValue} associated with the property name
	 */
	@JsonIgnore
	public void setProperty(String name, PropertyValue<? extends PropertyDataType> value) {
		this.map.put(name, value);
	}

	/**
	 * Removes a property based on the property name
	 *
	 * @param name the name of the property to remove
	 */
	@JsonIgnore
	public void removeProperty(String name) {
		this.map.remove(name);
	}

	/**
	 * Clears all properties and values from the {@link PropertySet}
	 */
	@JsonIgnore
	public void clear() {
		this.map.clear();
	}

	/**
	 * Gets the names of the {@link PropertySet}
	 *
	 * @return the names of the {@link PropertySet}
	 */
	@JsonIgnore
	public Set<String> getNames() {
		return map.keySet();
	}

	/**
	 * Gets the values of the {@link PropertySet}
	 *
	 * @return the values of the {@link PropertySet}
	 */
	@JsonIgnore
	public Collection<PropertyValue<? extends PropertyDataType>> getValues() {
		return map.values();
	}

	/**
	 * Gets the {@link Map} of {@link String} property names to {@link PropertyValue}s
	 *
	 * @return the {@link Map} of {@link String} property names to {@link PropertyValue}s
	 */
	@JsonIgnore
	public Map<String, PropertyValue<? extends PropertyDataType>> getPropertyMap() {
		return map;
	}

	@Override
	public String toString() {
		return "PropertySet [propertyMap=" + map + "]";
	}

	@Override
	public int size() {
		return map.size();
	}

	@Override
	public boolean isEmpty() {
		return map.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return map.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return map.containsValue(value);
	}

	@Override
	public PropertyValue<? extends PropertyDataType> get(Object key) {
		return map.get(key);
	}

	@Override
	public PropertyValue<? extends PropertyDataType> put(String key, PropertyValue<? extends PropertyDataType> value) {
		return map.put(key, value);
	}

	@Override
	public PropertyValue<? extends PropertyDataType> remove(Object key) {
		return map.remove(key);
	}

	@Override
	public void putAll(Map<? extends String, ? extends PropertyValue<? extends PropertyDataType>> m) {
		map.putAll(m);
	}

	@Override
	public Set<String> keySet() {
		return map.keySet();
	}

	@Override
	public Collection<PropertyValue<? extends PropertyDataType>> values() {
		return map.values();
	}

	@Override
	public Set<java.util.Map.Entry<String, PropertyValue<? extends PropertyDataType>>> entrySet() {
		return map.entrySet();
	}

	/**
	 * A builder for a PropertySet instance
	 */
	public static class PropertySetBuilder {

		private Map<String, PropertyValue<? extends PropertyDataType>> propertyMap;

		public PropertySetBuilder() {
			this.propertyMap = new HashMap<>();
		}

		public PropertySetBuilder(Map<String, PropertyValue<? extends PropertyDataType>> propertyMap) {
			this.propertyMap = propertyMap;
		}

		public PropertySetBuilder(PropertySet propertySet) throws SparkplugInvalidTypeException {
			this.propertyMap = new HashMap<>();
			for (String name : propertySet.getNames()) {
				PropertyValue<? extends PropertyDataType> value = propertySet.getPropertyValue(name);
				propertyMap.put(name, new PropertyValue<>(value.getType(), value.getValue()));
			}
		}

		public PropertySetBuilder addProperty(String name, PropertyValue<? extends PropertyDataType> value) {
			this.propertyMap.put(name, value);
			return this;
		}

		public PropertySetBuilder addProperties(Map<String, PropertyValue<? extends PropertyDataType>> properties) {
			this.propertyMap.putAll(properties);
			return this;
		}

		public PropertySet createPropertySet() {
			return new PropertySet(propertyMap);
		}
	}
}
