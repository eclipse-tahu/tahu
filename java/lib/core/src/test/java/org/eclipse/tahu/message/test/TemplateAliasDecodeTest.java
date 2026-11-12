/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0
 ********************************************************************************/

package org.eclipse.tahu.message.test;

import org.eclipse.tahu.message.SparkplugBPayloadDecoder;
import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.MetricDataType;
import org.eclipse.tahu.message.model.SparkplugBPayload;
import org.eclipse.tahu.message.model.Template;
import org.eclipse.tahu.model.MetricMap;
import org.eclipse.tahu.protobuf.SparkplugBProto;

import junit.framework.TestCase;

/**
 * A Template in a data message may be addressed by its alias and have members without a datatype: those are resolved
 * through the name of the Template in the birth.
 */
public class TemplateAliasDecodeTest extends TestCase {

	public void testTemplateAddressedByAlias() throws Exception {
		MetricMap birth = new MetricMap();
		birth.addAlias("Pump", 5L, MetricDataType.Template);
		birth.addAlias("Pump/speed", null, MetricDataType.Int32);

		SparkplugBProto.Payload data = SparkplugBProto.Payload.newBuilder().setSeq(1)
				.addMetrics(SparkplugBProto.Payload.Metric.newBuilder().setAlias(5)
						.setTemplateValue(SparkplugBProto.Payload.Template.newBuilder().setIsDefinition(false)
								.setTemplateRef("Type").addMetrics(SparkplugBProto.Payload.Metric.newBuilder()
										.setName("speed").setIntValue(42))))
				.build();

		SparkplugBPayload payload =
				new SparkplugBPayloadDecoder().buildFromByteArray(data.toByteArray(), birth.getMetricDataTypeMap());

		assertEquals(1, payload.getMetrics().size());
		Template template = (Template) payload.getMetrics().get(0).getValue();
		Metric member = template.getMetrics().get(0);
		assertEquals("speed", member.getName());
		assertEquals(42, member.getValue());
	}
}
