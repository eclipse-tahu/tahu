/********************************************************************************
 * Copyright (c) 2022 Cirrus Link Solutions and others
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.eclipse.tahu.util.SparkplugUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * A class representing a Sparkplug B payload as a {@link Map} to prevent duplication of {@link Metric}s. This can be
 * useful for Sparkplug BIRTH payloads.
 * <p>
 * Template definitions are kept apart from every other {@link Metric}. A Sparkplug BIRTH carries a Template
 * definition under the name an instance's template reference points at, and nothing stops a Tag from carrying that
 * same name - an instance named after its own type is the common case. The two are different things to the receiving
 * end, which separates definitions from the rest before it resolves anything by name, so they can share a name here
 * as well. {@link #getTemplateDefinition(String)} and {@link #removeTemplateDefinition(String)} address the
 * definitions alone. {@link #getMetric(String)} and {@link #removeMetric(String)} address the Tags, and fall back to
 * the definition of that name when there is no Tag, so a caller that looked a definition up by name before the two
 * were kept apart still finds it. {@link #getMetrics()} returns the definitions first, so a receiver that handles the
 * payload in order meets a definition before any instance of it.
 */
public class SparkplugBPayloadMap extends SparkplugBPayload {

	private static Logger logger = LoggerFactory.getLogger(SparkplugBPayloadMap.class.getName());

	private final ConcurrentHashMap<String, Metric> metricMap;

	private final ConcurrentHashMap<String, Metric> templateDefinitionMap;

	private final Object mapLock = new Object();

	/**
	 * Default Constructor
	 */
	public SparkplugBPayloadMap() {
		super();
		metricMap = new ConcurrentHashMap<>();
		templateDefinitionMap = new ConcurrentHashMap<>();
	}

	/**
	 * Constructor
	 *
	 * @param timestamp the overall {@link Date} timestamp of the {@link SparkplugBPayload}
	 * @param metrics a {@link List} of {@link Metrics} in the {@link SparkplugBPayload}
	 * @param seq the Sparkplug sequence number for the {@link SparkplugBPayload}
	 * @param uuid a UUID for the {@link SparkplugBPayload}
	 * @param body an array of bytes for the {@link SparkplugBPayload}
	 */
	public SparkplugBPayloadMap(Date timestamp, List<Metric> metrics, long seq, String uuid, byte[] body) {
		super(timestamp, null, seq, uuid, body);
		metricMap = new ConcurrentHashMap<>();
		templateDefinitionMap = new ConcurrentHashMap<>();
		for (Metric metric : metrics) {
			mapFor(metric).put(metric.getName(), metric);
		}
	}

	/*
	 * The map a Metric belongs in - Template definitions in one, everything else in the other
	 */
	private ConcurrentHashMap<String, Metric> mapFor(Metric metric) {
		return SparkplugUtil.isTemplateDefinition(metric) ? templateDefinitionMap : metricMap;
	}

	/**
	 * Adds a {@link Metric} to the {@link SparkplugBPayload}. If the {@link Metric} is already present with the same
	 * name, it will be replaced. A Template definition replaces only a Template definition of the same name, never a
	 * Tag of that name, and the other way round.
	 *
	 * metric the {@link Metric} to add
	 */
	@Override
	public void addMetric(Metric metric) {
		synchronized (mapLock) {
			mapFor(metric).put(metric.getName(), metric);
		}
	}

	/**
	 * Adds a {@link Metric} to the {@link SparkplugBPayload}. If the {@link Metric} is already present with the same
	 * name, it will be replaced - see {@link #addMetric(Metric)}.
	 *
	 * index this is ignored for this implementation metric the {@link Metric} to add
	 */
	@Override
	public void addMetric(int index, Metric metric) {
		addMetric(metric);
	}

	/**
	 * Adds a {@link List} of {@link Metric}s to the {@link SparkplugBPayloadMap}. If the list of {@link Metric}s has
	 * metrics with duplicate names, only the last one in the {@link List} will be included in the
	 * {@link SparkplugBPayloadMap}
	 *
	 * metrics a {@link List} of {@link Metric}s to add to the {@link SparkplugBPayloadMap}
	 */
	@Override
	public void addMetrics(List<Metric> metrics) {
		synchronized (mapLock) {
			for (Metric metric : metrics) {
				addMetric(metric);
			}
		}
	}

	/**
	 * Not used for the {@link SparkplugBPayloadMap}. This will always do nothing and return null.
	 *
	 * index not used
	 */
	@Override
	public Metric removeMetric(int index) {
		// This method isn't valid for the SparkplugBPayloadMap
		logger.error("removeMetric(int index) isn't supported by the SparkplugBPayloadMap");
		return null;
	}

	/**
	 * Removes a {@link Metric} by name - a Template definition from the definitions, anything else from the Tags.
	 *
	 * @param metric the {@link Metric} to remove
	 * @return true if the {@link Metric} was removed, otherwise false
	 */
	@Override
	public boolean removeMetric(Metric metric) {
		synchronized (mapLock) {
			if (metric != null) {
				return SparkplugUtil.isTemplateDefinition(metric)
						? removeTemplateDefinition(metric.getName())
						: removeMetric(metric.getName());
			}

			return false;
		}
	}

	/**
	 * Removes a {@link Metric} by metric name. The Tag of that name is removed when there is one; otherwise the
	 * Template definition of that name is, as it was before definitions were kept apart. To remove a definition
	 * regardless of a Tag of the same name, see {@link #removeTemplateDefinition(String)}.
	 *
	 * @param metricName the {@link String} metricName to remove
	 * @return true if a {@link Metric} was removed, otherwise false
	 */
	public boolean removeMetric(String metricName) {
		synchronized (mapLock) {
			if (metricName == null) {
				return false;
			}
			if (metricMap.remove(metricName) != null) {
				return true;
			}
			return templateDefinitionMap.remove(metricName) != null;
		}
	}

	/**
	 * Removes a Template definition by name.
	 *
	 * @param templateName the name of the Template definition to remove
	 * @return true if the Template definition was removed, otherwise false
	 */
	public boolean removeTemplateDefinition(String templateName) {
		synchronized (mapLock) {
			return templateName != null && templateDefinitionMap.remove(templateName) != null;
		}
	}

	/**
	 * Gets a {@link List} of {@link Metric}s in the {@link SparkplugBPayloadMap} - the Template definitions first, then
	 * everything else
	 */
	@Override
	public List<Metric> getMetrics() {
		List<Metric> metrics = new ArrayList<>(templateDefinitionMap.size() + metricMap.size());
		metrics.addAll(templateDefinitionMap.values());
		metrics.addAll(metricMap.values());
		return metrics;
	}

	/**
	 * Gets a {@link List} of the Template definitions in the {@link SparkplugBPayloadMap}
	 *
	 * @return the Template definition {@link Metric}s
	 */
	@JsonIgnore
	public List<Metric> getTemplateDefinitions() {
		return new ArrayList<>(templateDefinitionMap.values());
	}

	/**
	 * Gets the number of {@link Metric}s in this {@link SparkplugBPayloadMap}, Template definitions included
	 */
	@Override
	@JsonIgnore
	public Integer getMetricCount() {
		return templateDefinitionMap.size() + metricMap.size();
	}

	/**
	 * Sets the {@link List} of {@link Metric}s for this {@link SparkplugBPayloadMap}
	 *
	 * @param metrics the {@link List} of {@link Metric}s to set for this {@link SparkplugBPayloadMap}
	 */
	@Override
	public void setMetrics(List<Metric> metrics) {
		synchronized (mapLock) {
			metricMap.clear();
			templateDefinitionMap.clear();
			for (Metric metric : metrics) {
				mapFor(metric).put(metric.getName(), metric);
			}
		}
	}

	/**
	 * Gets a Metric for a given metric name. The Tag of that name is returned when there is one; otherwise the
	 * Template definition of that name is, as it was before definitions were kept apart. To fetch a definition
	 * regardless of a Tag of the same name, see {@link #getTemplateDefinition(String)}.
	 *
	 * @param metricName the name of the {@link Metric} to fetch
	 *
	 * @return the {@link Metric} with the provided metric name, or null if there is none
	 */
	public Metric getMetric(String metricName) {
		if (metricMap == null || metricName == null) {
			return null;
		}
		Metric metric = metricMap.get(metricName);
		return metric != null ? metric : getTemplateDefinition(metricName);
	}

	/**
	 * Gets the Template definition for a given name
	 *
	 * @param templateName the name of the Template definition to fetch
	 * @return the Template definition {@link Metric} with the provided name, or null if there is none
	 */
	public Metric getTemplateDefinition(String templateName) {
		return templateDefinitionMap == null || templateName == null ? null : templateDefinitionMap.get(templateName);
	}

	/**
	 * Updates a {@link Metric} value in the {@link SparkplugBPayloadMap}. The update lands on the Tag of that name; a
	 * Template definition passed in replaces the definition of its name and never becomes a Tag.
	 * 
	 * @param metricName the name of the metric. This is required as Aliasing may be enabled and the name may not be set
	 *            in the {@link Metric}
	 * @param metric the {@link Metric} to update the value of
	 */
	public void updateMetricValue(String newMetricName, Metric newMetric, List<Property<?>> customProperties) {
		if (newMetric == null) {
			logger.info("Metric '{}' is null during update - removing from cache", newMetricName);
			metricMap.put(newMetricName, null);
			return;
		}

		if (SparkplugUtil.isTemplateDefinition(newMetric)) {
			// A definition has no value to update - a new definition replaces the one of that name
			logger.trace("Replacing the Template definition in the map: {}", newMetric);
			addMetric(newMetric);
			return;
		}

		Metric existingMetric = metricMap.get(newMetricName);

		try {
			// Update the 'qualified value' which is the value, quality, and timestamp
			if (existingMetric != null) {
				if (newMetric.getDataType() == MetricDataType.Template && newMetric.getValue() != null) {
					TemplateMap templateMap = null;
					if (TemplateMap.class.isAssignableFrom(existingMetric.getValue().getClass())) {
						templateMap = (TemplateMap) (existingMetric.getValue());
					} else {
						templateMap = new TemplateMap((Template) (existingMetric.getValue()));
						existingMetric.setValue(templateMap);
					}
					updateTemplateMetricValues(templateMap, newMetric, customProperties);
				} else {
					existingMetric.setValue(newMetric.getValue());
				}

				handleProps(existingMetric, newMetric, customProperties);
				logger.trace("Updated metric in the map: {}", existingMetric);
			} else {
				logger.trace("Adding new metric to cache when updating: {}", newMetric);
				metricMap.put(newMetricName, newMetric);
			}
		} catch (Exception e) {
			logger.error("Failed to update metric value for {}: {} with customProps={}", newMetricName, newMetric,
					customProperties, e);
		}
	}

	private void updateTemplateMetricValues(TemplateMap existingTemplateMap, Metric newMetric,
			List<Property<?>> customProperties) {
		try {
			Template newTemplate = (Template) newMetric.getValue();
			List<Metric> newMemberMetrics = newTemplate.getMetrics();
			if (newMemberMetrics != null && !newMemberMetrics.isEmpty()) {
				for (Metric newMemberMetric : newMemberMetrics) {
					Metric existingMetric = existingTemplateMap.getMetricMap().get(newMemberMetric.getName());
					if (newMemberMetric.getDataType() == MetricDataType.Template
							&& newMemberMetric.getValue() != null) {
						TemplateMap templateMap = null;
						if (TemplateMap.class.isAssignableFrom(existingMetric.getValue().getClass())) {
							templateMap = (TemplateMap) (existingMetric.getValue());
						} else {
							templateMap = new TemplateMap((Template) (existingMetric.getValue()));
							existingMetric.setValue(templateMap);
						}
						updateTemplateMetricValues(templateMap, newMemberMetric, customProperties);
					} else {
						existingTemplateMap.getMetricMap().get(newMemberMetric.getName())
								.setValue(newMemberMetric.getValue());
					}

					handleProps(existingMetric, newMemberMetric, customProperties);
				}
			}
		} catch (Exception e) {
			logger.error("Failed to update Template metric value for {}: {} with customProps={}", newMetric.getName(),
					newMetric, customProperties, e);
		}
	}

	private void handleProps(Metric existingMetric, Metric newMetric, List<Property<?>> customProperties) {
		PropertySet props = existingMetric.getProperties();
		if (newMetric.getProperties() != null
				&& newMetric.getProperties().getPropertyValue(SparkplugMeta.QUALITY_PROP_NAME) != null) {
			if (props == null) {
				props = new PropertySet();
				existingMetric.setProperties(props);
			}
			props.setProperty(SparkplugMeta.QUALITY_PROP_NAME,
					newMetric.getProperties().getPropertyValue(SparkplugMeta.QUALITY_PROP_NAME));
		} else {
			if (props != null) {
				// If there is no quality - it is implied good and should be updated as such by simply removing it
				props.remove(SparkplugMeta.QUALITY_PROP_NAME);
			}
		}
		existingMetric.setTimestamp(newMetric.getTimestamp());

		if (customProperties != null && !customProperties.isEmpty()) {
			for (Property<?> customProperty : customProperties) {
				if (newMetric.getProperties() != null
						&& newMetric.getProperties().getPropertyValue(customProperty.getName()) != null) {
					if (props == null) {
						props = new PropertySet();
						existingMetric.setProperties(props);
					}
					props.setProperty(customProperty.getName(),
							newMetric.getProperties().getPropertyValue(customProperty.getName()));
				}
			}
		}
	}

	/**
	 * Updates all {@link Metric} timestamps to the specified {@link Date} as well as the timestamp for the overall
	 * {@link SparkplugBPayloadMap}
	 *
	 * @param date the {@link Date} timestamp to use for all {@link Metric}s in this {@link SparkplugBPayloadMap}
	 */
	public void updateMetricTimestamps(Date date) {
		for (Metric metric : getMetrics()) {
			metric.setTimestamp(date);
			if (metric.getDataType() == MetricDataType.Template && metric.getValue() != null) {
				updateTemplateTimestamps((Template) metric.getValue(), date);
			}
		}
	}

	private void updateTemplateTimestamps(Template template, Date date) {
		if (template != null && template.getMetrics() != null) {
			for (Metric metric : template.getMetrics()) {
				metric.setTimestamp(date);
				if (metric.getDataType() == MetricDataType.Template && metric.getValue() != null) {
					updateTemplateTimestamps((Template) metric.getValue(), date);
				}
			}
		}
	}

	public void uptickMetricTimestamps(Date birthTimestamp) {
		for (Metric metric : getMetrics()) {
			Date uptickedTimestamp = new Date(metric.getTimestamp().getTime() + 1);
			if (birthTimestamp.before(uptickedTimestamp)) {
				metric.setTimestamp(birthTimestamp);
			} else {
				metric.setTimestamp(uptickedTimestamp);
			}
			if (metric.getDataType() == MetricDataType.Template && metric.getValue() != null) {
				uptickTemplateTimestamps((Template) metric.getValue(), birthTimestamp);
			}
		}
	}

	private void uptickTemplateTimestamps(Template template, Date birthTimestamp) {
		if (template != null && template.getMetrics() != null) {
			for (Metric metric : template.getMetrics()) {
				Date uptickedTimestamp = new Date(metric.getTimestamp().getTime() + 1);
				if (birthTimestamp.before(uptickedTimestamp)) {
					metric.setTimestamp(birthTimestamp);
				} else {
					metric.setTimestamp(uptickedTimestamp);
				}
				if (metric.getDataType() == MetricDataType.Template && metric.getValue() != null) {
					updateTemplateTimestamps((Template) metric.getValue(), birthTimestamp);
				}
			}
		}
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("SparkplugBPayloadMap [timestamp=");
		builder.append(super.getTimestamp() != null ? super.getTimestamp().getTime() : "null");
		builder.append(", metrics=");
		builder.append(getMetrics());
		builder.append(", seq=");
		builder.append(super.getSeq());
		builder.append(", uuid=");
		builder.append(super.getUuid());
		builder.append(", body=");
		builder.append(Arrays.toString(super.getBody()));
		builder.append("]");
		return builder.toString();
	}

	/**
	 * A builder for creating a {@link SparkplugBPayloadMapBuilder} instance.
	 */
	public static class SparkplugBPayloadMapBuilder {

		private Date timestamp;
		private List<Metric> metrics;
		private long seq = -1;
		private String uuid;
		private byte[] body;

		public SparkplugBPayloadMapBuilder(long sequenceNumber) {
			this.seq = sequenceNumber;
			metrics = new ArrayList<Metric>();
		}

		public SparkplugBPayloadMapBuilder() {
			metrics = new ArrayList<Metric>();
		}

		public SparkplugBPayloadMapBuilder addMetric(Metric metric) {
			this.metrics.add(metric);
			return this;
		}

		public SparkplugBPayloadMapBuilder addMetrics(Collection<Metric> metrics) {
			this.metrics.addAll(metrics);
			return this;
		}

		public SparkplugBPayloadMapBuilder setTimestamp(Date timestamp) {
			this.timestamp = timestamp;
			return this;
		}

		public SparkplugBPayloadMapBuilder setSeq(long seq) {
			this.seq = seq;
			return this;
		}

		public SparkplugBPayloadMapBuilder setUuid(String uuid) {
			this.uuid = uuid;
			return this;
		}

		public SparkplugBPayloadMapBuilder setBody(byte[] body) {
			this.body = body;
			return this;
		}

		public SparkplugBPayloadMap createPayload() {
			return new SparkplugBPayloadMap(timestamp, metrics, seq, uuid, body);
		}
	}
}
