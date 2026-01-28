/********************************************************************************
 * Copyright (c) 2013-2022 Cirrus Link Solutions and others
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

package org.eclipse.tahu.json;

import java.io.IOException;
import java.util.Base64;

import org.eclipse.tahu.message.model.File;
import org.eclipse.tahu.message.model.MetaData;
import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.MetricDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.deser.ResolvableDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

/**
 * A custom JSON deserializer for {@link Metric} instances.
 */
public class MetricDeserializer extends StdDeserializer<Metric> implements ResolvableDeserializer {

	private static final Logger logger = LoggerFactory.getLogger(MetricDeserializer.class.getName());

	private static final long serialVersionUID = 1L;

	private final JsonDeserializer<?> defaultDeserializer;

	/**
	 * Constructor.
	 */
	protected MetricDeserializer(JsonDeserializer<?> defaultDeserializer) {
		super(Metric.class);
		this.defaultDeserializer = defaultDeserializer;
	}

	@Override
	public Metric deserialize(JsonParser parser, DeserializationContext ctxt)
			throws IOException, JsonProcessingException {

		Metric metric = (Metric) defaultDeserializer.deserialize(parser, ctxt);

		// Check if the data type is a File
		if (MetricDataType.File.equals(metric.getDataType())) {
			// Perform the custom logic for File types by building up the File object.
			MetaData metaData = metric.getMetaData();
			String fileName = metaData == null ? null : metaData.getFileName();
			File file = new File(fileName, Base64.getDecoder().decode((String) metric.getValue()));
			metric.setValue(file);
		}

		return metric;
	}

	@Override
	public void resolve(DeserializationContext ctxt) throws JsonMappingException {
		((ResolvableDeserializer) defaultDeserializer).resolve(ctxt);
	}
}
