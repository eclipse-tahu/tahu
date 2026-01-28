/********************************************************************************
 * Copyright (c) 2023 Cirrus Link Solutions and others
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

import org.eclipse.tahu.message.model.Template;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.deser.ResolvableDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

/**
 * Defines Template deserializer
 */
public class TemplateDeserializer extends StdDeserializer<Template> implements ResolvableDeserializer {

	private static Logger logger = LoggerFactory.getLogger(TemplateDeserializer.class.getName());

	private static final long serialVersionUID = 1L;

	private final JsonDeserializer<?> defaultDeserializer;

	/**
	 * Constructor.
	 */
	protected TemplateDeserializer(JsonDeserializer<?> defaultDeserializer) {
		super(Template.class);
		this.defaultDeserializer = defaultDeserializer;
	}

	@Override
	public void resolve(DeserializationContext ctxt) throws JsonMappingException {
		((ResolvableDeserializer) defaultDeserializer).resolve(ctxt);
	}

	@Override
	public Template deserialize(JsonParser parser, DeserializationContext ctxt) throws IOException, JacksonException {
		Template template = (Template) defaultDeserializer.deserialize(parser, ctxt);
		return template;
	}
}
