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

package org.eclipse.tahu.util;

import java.math.BigInteger;

import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.MetricDataType;
import org.eclipse.tahu.message.model.PropertySet;
import org.eclipse.tahu.message.model.SparkplugBPayload;
import org.eclipse.tahu.message.model.SparkplugMeta;
import org.eclipse.tahu.message.model.Template;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkplugUtil {

	private static Logger logger = LoggerFactory.getLogger(SparkplugUtil.class.getName());

	/**
	 * Reports if a {@link Metric} is a Template definition, as opposed to a Template instance or any other
	 * {@link Metric}.
	 *
	 * @param metric the {@link Metric} to inspect
	 * @return true if the {@link Metric} carries a Template definition, otherwise false
	 */
	public static boolean isTemplateDefinition(Metric metric) {
		return metric != null && MetricDataType.Template.equals(metric.getDataType())
				&& metric.getValue() instanceof Template && ((Template) metric.getValue()).isDefinition();
	}

	public static int getQualityCode(Metric metric) {
		PropertySet propertySet = metric.getProperties();
		logger.trace("Getting properties for {} with value: {}", metric.getName(),
				(propertySet != null && propertySet.getPropertyMap() != null)
						? propertySet.getPropertyMap().toString()
						: "null");
		if (propertySet != null && propertySet.getPropertyValue("Quality") != null) {
			return (int) propertySet.getPropertyValue("Quality").getValue();
		}

		logger.trace("No incoming quality for {} - assuming good", metric.getName());
		return 192;
	}

	public static Long getBdSequenceNumber(SparkplugBPayload payload) throws Exception {
		for (Metric metric : payload.getMetrics()) {
			if (SparkplugMeta.SPARKPLUG_BD_SEQUENCE_NUMBER_KEY.equals(metric.getName())) {
				return convertSeqNumber(metric.getValue());
			}
		}

		// No BD sequence number found - return null
		return null;
	}

	public static long convertSeqNumber(Object sequenceNumber) {
		if (sequenceNumber instanceof Long) {
			return (long) sequenceNumber;
		} else if (sequenceNumber instanceof BigInteger) {
			return ((BigInteger) sequenceNumber).longValue();
		} else if (sequenceNumber instanceof Integer) {
			return ((Integer) sequenceNumber).longValue();
		} else if (sequenceNumber instanceof Byte) {
			return ((Byte) sequenceNumber).longValue();
		} else if (sequenceNumber instanceof Short) {
			return ((Short) sequenceNumber).longValue();
		}
		// Default to explicit cast to long
		return (long) sequenceNumber;
	}
}
