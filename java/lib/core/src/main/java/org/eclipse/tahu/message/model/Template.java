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
import java.util.Collection;
import java.util.List;

import org.eclipse.tahu.SparkplugException;
import org.eclipse.tahu.message.model.Metric.MetricBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

/**
 * A class representing a template associated with a metric
 */
@JsonInclude(Include.NON_NULL)
public class Template {

	private static Logger logger = LoggerFactory.getLogger(Template.class.getName());

	/**
	 * The Template version.
	 */
	@JsonProperty("version")
	private String version;

	/**
	 * The template reference
	 */
	@JsonProperty("reference")
	private String templateRef;

	/**
	 * True if the template is a definition, false otherwise.
	 */
	@JsonProperty("isDefinition")
	private boolean isDefinition;

	/**
	 * List of metrics.
	 */
	@JsonProperty("metrics")
	private List<Metric> metrics;

	/**
	 * List of parameters.
	 */
	@JsonProperty("parameters")
	@JsonInclude(Include.NON_EMPTY)
	private List<Parameter> parameters;

	public Template() {
	}

	/**
	 * Constructor
	 * 
	 * @param name the template name
	 * @param version the template version
	 * @param templateRef a template reference
	 * @param isDefinition a flag indicating if this is a template definition
	 * @param metrics a list of metrics
	 * @param parmeters a list of parameters
	 */
	public Template(String version, String templateRef, boolean isDefinition, List<Metric> metrics,
			List<Parameter> parameters) {
		this.version = version;
		this.templateRef = templateRef;
		this.isDefinition = isDefinition;
		this.metrics = metrics;
		this.parameters = parameters;
	}

	/**
	 * Copy Constructor
	 *
	 * @param template the {@link Template} to copy
	 * @throws Exception if the instantiation can not complete
	 */
	public Template(Template template) throws Exception {
		this.version = template.getVersion();
		this.templateRef = template.getTemplateRef();
		this.isDefinition = template.isDefinition();
		if (template.getMetrics() != null) {
			this.metrics = new ArrayList<>();
			for (Metric metric : template.getMetrics()) {
				this.metrics.add(new Metric(metric));
			}
		}
		if (template.getParameters() != null) {
			this.parameters = new ArrayList<>();
			for (Parameter parameter : template.getParameters()) {
				this.parameters.add(new Parameter(parameter));
			}
		}
	}

	/**
	 * Gets the version of this {@link Template}
	 *
	 * @return the version of this {@link Template}
	 */
	public String getVersion() {
		return version;
	}

	/**
	 * Sets the version of this {@link Template}
	 *
	 * @param version the version to set for this {@link Template}
	 */
	public void setVersion(String version) {
		this.version = version;
	}

	/**
	 * Gets the template reference of this {@link Template}
	 *
	 * @return the template reference of this {@link Template}
	 */
	public String getTemplateRef() {
		return templateRef;
	}

	/**
	 * Sets the template reference of this {@link Template}
	 *
	 * @param templateRef the template reference to set for this {@link Template}
	 */
	public void setTemplateRef(String templateRef) {
		this.templateRef = templateRef;
	}

	/**
	 * Gets whether or not this {@link Template} is a definition or not
	 *
	 * @return true if this is a definition, otherwise false (meaning it is an instance)
	 */
	@JsonGetter("isDefinition")
	public boolean isDefinition() {
		return isDefinition;
	}

	/**
	 * Sets whether or not this {@link Template} is a definition or not
	 *
	 * @param isDefinition a boolean donoting if this is a {@link Template} definition or instance
	 */
	@JsonSetter("isDefinition")
	public void setDefinition(boolean isDefinition) {
		this.isDefinition = isDefinition;
	}

	/**
	 * Gets the {@link List} of {@link Metric}s associated with the {@link Template}
	 *
	 * @return the {@link List} of {@link Metric}s associated with the {@link Template}
	 */
	public List<Metric> getMetrics() {
		return metrics;
	}

	/**
	 * Sets the {@link List} of {@link Metric}s for this {@link Template}
	 *
	 * @param metrics the {@link List} of {@link Metric}s to set for this {@link Template}
	 */
	public void setMetrics(List<Metric> metrics) {
		this.metrics = metrics;
	}

	/**
	 * Adds a {@link Metric} to the end of the {@link List} of Sparkplug metrics
	 *
	 * @param metric a {@link Metric} to add to the end of the {@link List} of Sparkplug metrics
	 */
	public void addMetric(Metric metric) {
		this.metrics.add(metric);
	}

	/**
	 * Gets the {@link List} of {@link Parameter}s associated with the {@link Template}
	 *
	 * @return the {@link List} of {@link Parameter}s associated with the {@link Template}
	 */
	public List<Parameter> getParameters() {
		return parameters;
	}

	/**
	 * Sets the {@link List} of {@link Parameter}s for this {@link Template}
	 *
	 * @param metrics the {@link List} of {@link Parameter}s to set for this {@link Template}
	 */
	public void setParameters(List<Parameter> parameters) {
		this.parameters = parameters;
	}

	/**
	 * Adds a {@link Parameter} to this {@link Template}
	 * 
	 * @param parameter a {@link Parameter} to add to this {@link Template}
	 */
	public void addParameter(Parameter parameter) {
		this.parameters.add(parameter);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Template [version=");
		builder.append(version);
		builder.append(", templateRef=");
		builder.append(templateRef);
		builder.append(", isDefinition=");
		builder.append(isDefinition);
		builder.append(", metrics=");
		builder.append(metrics);
		builder.append(", parameters=");
		builder.append(parameters);
		builder.append("]");
		return builder.toString();
	}

	/**
	 * A builder for creating a {@link Template} instance.
	 */
	public static class TemplateBuilder {

		private String version;
		private String templateRef;
		private boolean isDefinition;
		private List<Metric> metrics;
		private List<Parameter> parameters;

		/**
		 * @param name
		 * @param version
		 * @param templateRef
		 * @param isDefinition
		 * @param metrics
		 * @param parameters
		 */
		public TemplateBuilder() {
			super();
			this.metrics = new ArrayList<Metric>();
			this.parameters = new ArrayList<Parameter>();
		}

		public TemplateBuilder(Template template) throws SparkplugException {
			this.version = template.getVersion();
			this.templateRef = template.getTemplateRef();
			this.isDefinition = template.isDefinition();
			this.metrics = new ArrayList<Metric>(template.getMetrics().size());
			for (Metric metric : template.getMetrics()) {
				this.metrics.add(new MetricBuilder(metric).createMetric());
			}
			this.parameters = new ArrayList<Parameter>(template.getParameters().size());
			for (Parameter parameter : template.getParameters()) {
				this.parameters.add(new Parameter(parameter.getName(), parameter.getType(), parameter.getValue()));
			}
		}

		public TemplateBuilder version(String version) {
			this.version = version;
			return this;
		}

		public TemplateBuilder templateRef(String templateRef) {
			this.templateRef = templateRef;
			return this;
		}

		public TemplateBuilder definition(boolean isDefinition) {
			this.isDefinition = isDefinition;
			return this;
		}

		public TemplateBuilder addMetric(Metric metric) {
			this.metrics.add(metric);
			return this;
		}

		public TemplateBuilder addMetrics(Collection<Metric> metrics) {
			this.metrics.addAll(metrics);
			return this;
		}

		public TemplateBuilder addParameter(Parameter parameter) {
			this.parameters.add(parameter);
			return this;
		}

		public TemplateBuilder addParameters(Collection<Parameter> parameters) {
			this.parameters.addAll(parameters);
			return this;
		}

		public Template createTemplate() {
			return new Template(version, templateRef, isDefinition, metrics, parameters);
		}
	}
}
