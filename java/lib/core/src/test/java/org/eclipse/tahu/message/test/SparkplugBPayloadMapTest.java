/********************************************************************************
 * Copyright (c) 2026 Cirrus Link Solutions and others
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

package org.eclipse.tahu.message.test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.eclipse.tahu.message.SparkplugBPayloadDecoder;
import org.eclipse.tahu.message.SparkplugBPayloadEncoder;
import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.Metric.MetricBuilder;
import org.eclipse.tahu.message.model.MetricDataType;
import org.eclipse.tahu.message.model.SparkplugBPayload;
import org.eclipse.tahu.message.model.SparkplugBPayloadMap;
import org.eclipse.tahu.message.model.Template;
import org.eclipse.tahu.message.model.Template.TemplateBuilder;
import org.eclipse.tahu.util.SparkplugUtil;
import org.testng.annotations.Test;

/**
 * Tests that a {@link SparkplugBPayloadMap} can hold a Template definition and a Tag of the same name at once.
 * <p>
 * A BIRTH carries a Template definition under the name an instance's template reference points at, and an instance
 * is routinely named after its own type - 'EdgeDevice' of type 'EdgeDevice'. The payload on the wire allows both; the
 * map used to be keyed on name alone, so whichever was added second replaced the first.
 */
public class SparkplugBPayloadMapTest {

	private static final String NAME = "EdgeDevice";

	@Test
	public void testADefinitionAndATagOfTheSameNameAreBothKept() throws Exception {
		Metric instance = instance(NAME, NAME);
		Metric definition = definition(NAME);

		SparkplugBPayloadMap map = new SparkplugBPayloadMap();
		map.addMetric(instance);
		map.addMetric(definition);

		assertSame(map.getMetric(NAME), instance, "getMetric addresses the Tag");
		assertSame(map.getTemplateDefinition(NAME), definition, "getTemplateDefinition addresses the definition");
		assertEquals(map.getMetricCount(), Integer.valueOf(2));
		assertEquals(map.getMetrics().size(), 2);
		assertEquals(map.getTemplateDefinitions(), Arrays.asList(definition));
	}

	@Test
	public void testTheOrderOfAdditionDoesNotMatter() throws Exception {
		Metric instance = instance(NAME, NAME);
		Metric definition = definition(NAME);

		SparkplugBPayloadMap map = new SparkplugBPayloadMap();
		map.addMetric(definition);
		map.addMetric(instance);

		assertSame(map.getMetric(NAME), instance);
		assertSame(map.getTemplateDefinition(NAME), definition);
	}

	@Test
	public void testDefinitionsComeFirstInTheMetricList() throws Exception {
		SparkplugBPayloadMap map = new SparkplugBPayloadMap();
		map.addMetric(new MetricBuilder("Pressure", MetricDataType.Int32, 1).createMetric());
		map.addMetric(instance(NAME, NAME));
		map.addMetric(definition(NAME));
		map.addMetric(definition("Performance"));

		List<Metric> metrics = map.getMetrics();
		assertEquals(metrics.size(), 4);
		assertTrue(SparkplugUtil.isTemplateDefinition(metrics.get(0)), "first is a definition");
		assertTrue(SparkplugUtil.isTemplateDefinition(metrics.get(1)), "second is a definition");
		assertFalse(SparkplugUtil.isTemplateDefinition(metrics.get(2)));
		assertFalse(SparkplugUtil.isTemplateDefinition(metrics.get(3)));
	}

	@Test
	public void testADefinitionReplacesOnlyADefinition() throws Exception {
		Metric instance = instance(NAME, NAME);
		Metric first = definition(NAME);
		Metric second = definition(NAME);

		SparkplugBPayloadMap map = new SparkplugBPayloadMap();
		map.addMetric(instance);
		map.addMetric(first);
		map.addMetric(second);

		assertSame(map.getTemplateDefinition(NAME), second, "the later definition replaced the earlier one");
		assertSame(map.getMetric(NAME), instance, "the Tag was never touched");
		assertEquals(map.getMetricCount(), Integer.valueOf(2));
	}

	@Test
	public void testRemovingTheTagLeavesTheDefinition() throws Exception {
		SparkplugBPayloadMap map = mapWithBoth();

		assertTrue(map.removeMetric(NAME));

		assertNotNull(map.getTemplateDefinition(NAME));
		assertSame(map.getMetric(NAME), map.getTemplateDefinition(NAME), "the name now means the definition");
		assertEquals(map.getMetricCount(), Integer.valueOf(1));
	}

	@Test
	public void testRemovingTheDefinitionLeavesTheTag() throws Exception {
		SparkplugBPayloadMap map = mapWithBoth();

		assertTrue(map.removeTemplateDefinition(NAME));

		assertNotNull(map.getMetric(NAME));
		assertNull(map.getTemplateDefinition(NAME));
		assertFalse(map.removeTemplateDefinition(NAME));
	}

	@Test
	public void testRemovingByMetricRoutesOnWhatTheMetricIs() throws Exception {
		Metric instance = instance(NAME, NAME);
		Metric definition = definition(NAME);
		SparkplugBPayloadMap map = new SparkplugBPayloadMap();
		map.addMetric(instance);
		map.addMetric(definition);

		assertTrue(map.removeMetric(definition));
		assertSame(map.getMetric(NAME), instance, "removing the definition by Metric left the Tag");
		assertNull(map.getTemplateDefinition(NAME));

		map.addMetric(definition);
		assertTrue(map.removeMetric(instance));
		assertSame(map.getTemplateDefinition(NAME), definition, "removing the Tag by Metric left the definition");
		assertSame(map.getMetric(NAME), definition, "which the name now falls back to");
	}

	@Test
	public void testTheListConstructorAndSetMetricsRouteTheSameWay() throws Exception {
		Metric instance = instance(NAME, NAME);
		Metric definition = definition(NAME);

		SparkplugBPayloadMap fromList =
				new SparkplugBPayloadMap(new Date(), Arrays.asList(instance, definition), 0L, null, null);
		assertSame(fromList.getMetric(NAME), instance);
		assertSame(fromList.getTemplateDefinition(NAME), definition);

		SparkplugBPayloadMap set = new SparkplugBPayloadMap();
		set.addMetric(new MetricBuilder("Stale", MetricDataType.Int32, 9).createMetric());
		set.setMetrics(Arrays.asList(definition, instance));
		assertNull(set.getMetric("Stale"), "setMetrics replaces everything");
		assertSame(set.getMetric(NAME), instance);
		assertSame(set.getTemplateDefinition(NAME), definition);
	}

	@Test
	public void testUpdatingTheTagNeverTouchesTheDefinition() throws Exception {
		Metric instance = instance(NAME, NAME);
		Metric definition = definition(NAME);
		SparkplugBPayloadMap map = new SparkplugBPayloadMap();
		map.addMetric(instance);
		map.addMetric(definition);

		Metric update = instance(NAME, NAME);
		((Template) update.getValue()).getMetrics().get(0).setValue(42);
		map.updateMetricValue(NAME, update, new ArrayList<>());

		Template updated = (Template) map.getMetric(NAME).getValue();
		assertEquals(updated.getMetrics().get(0).getValue(), 42);
		assertTrue(((Template) map.getTemplateDefinition(NAME).getValue()).isDefinition(),
				"the definition is still a definition");
		assertEquals(((Template) map.getTemplateDefinition(NAME).getValue()).getMetrics().get(0).getValue(), 0,
				"the definition's member kept its default");
	}

	@Test
	public void testTimestampUpdatesReachTheDefinitions() throws Exception {
		SparkplugBPayloadMap map = mapWithBoth();
		Date stamp = new Date(5000L);

		map.updateMetricTimestamps(stamp);

		assertEquals(map.getMetric(NAME).getTimestamp(), stamp);
		assertEquals(map.getTemplateDefinition(NAME).getTimestamp(), stamp);
	}

	/**
	 * The point of keeping both: they go out on the wire together, and come back as two metrics of one name - one a
	 * definition, one not.
	 */
	@Test
	public void testBothSurviveEncodingAndDecoding() throws Exception {
		SparkplugBPayloadMap map = mapWithBoth();
		map.setTimestamp(new Date());
		map.setSeq(0L);

		byte[] bytes = new SparkplugBPayloadEncoder().getBytes(map, false);
		SparkplugBPayload decoded = new SparkplugBPayloadDecoder().buildFromByteArray(bytes, null);

		assertEquals(decoded.getMetrics().size(), 2);
		int definitions = 0;
		int instances = 0;
		for (Metric metric : decoded.getMetrics()) {
			assertEquals(metric.getName(), NAME);
			if (SparkplugUtil.isTemplateDefinition(metric)) {
				definitions++;
			} else {
				assertEquals(((Template) metric.getValue()).getTemplateRef(), NAME);
				instances++;
			}
		}
		assertEquals(definitions, 1);
		assertEquals(instances, 1);

		SparkplugBPayloadMap rebuilt = new SparkplugBPayloadMap(decoded.getTimestamp(), decoded.getMetrics(),
				decoded.getSeq(), decoded.getUuid(), decoded.getBody());
		assertNotNull(rebuilt.getMetric(NAME));
		assertNotNull(rebuilt.getTemplateDefinition(NAME));
	}

	@Test
	public void testIsTemplateDefinition() throws Exception {
		assertTrue(SparkplugUtil.isTemplateDefinition(definition(NAME)));
		assertFalse(SparkplugUtil.isTemplateDefinition(instance(NAME, NAME)));
		assertFalse(SparkplugUtil
				.isTemplateDefinition(new MetricBuilder(NAME, MetricDataType.Int32, 1).createMetric()));
		assertFalse(SparkplugUtil.isTemplateDefinition(null));
	}

	/**
	 * A caller that looked a definition up by name before definitions were kept apart still finds it, as long as no
	 * Tag has taken the name. With both present, the name means the Tag.
	 */
	@Test
	public void testLookingUpByNameFallsBackToTheDefinition() throws Exception {
		SparkplugBPayloadMap map = new SparkplugBPayloadMap();
		Metric definition = definition(NAME);
		map.addMetric(definition);

		assertSame(map.getMetric(NAME), definition, "no Tag of that name, so the definition is found");

		Metric instance = instance(NAME, NAME);
		map.addMetric(instance);
		assertSame(map.getMetric(NAME), instance, "the Tag takes the name once there is one");
		assertSame(map.getTemplateDefinition(NAME), definition);
	}

	@Test
	public void testRemovingByNameFallsBackToTheDefinition() throws Exception {
		SparkplugBPayloadMap map = mapWithBoth();

		assertTrue(map.removeMetric(NAME), "removes the Tag");
		assertNotNull(map.getTemplateDefinition(NAME));
		assertSame(map.getMetric(NAME), map.getTemplateDefinition(NAME), "only the definition is left under the name");

		assertTrue(map.removeMetric(NAME), "no Tag left, so the definition goes");
		assertNull(map.getTemplateDefinition(NAME));
		assertFalse(map.removeMetric(NAME), "nothing left under the name");
	}

	@Test
	public void testUptickingTimestampsReachesTheDefinitions() throws Exception {
		SparkplugBPayloadMap map = mapWithBoth();
		Date stamp = new Date(5000L);
		map.updateMetricTimestamps(stamp);
		Date birth = new Date(9000L);

		map.uptickMetricTimestamps(birth);

		assertEquals(map.getMetric(NAME).getTimestamp(), new Date(5001L));
		assertEquals(map.getTemplateDefinition(NAME).getTimestamp(), new Date(5001L));
	}

	@Test
	public void testTheIndexOverloadRoutesTheSameWay() throws Exception {
		SparkplugBPayloadMap map = new SparkplugBPayloadMap();
		map.addMetric(0, instance(NAME, NAME));
		map.addMetric(0, definition(NAME));

		assertFalse(((Template) map.getMetric(NAME).getValue()).isDefinition());
		assertTrue(((Template) map.getTemplateDefinition(NAME).getValue()).isDefinition());
		assertEquals(map.getMetricCount(), Integer.valueOf(2));
	}

	/**
	 * A definition handed to updateMetricValue replaces the definition of its name, and never lands in the Tags.
	 */
	@Test
	public void testUpdatingWithADefinitionReplacesOnlyTheDefinition() throws Exception {
		SparkplugBPayloadMap map = mapWithBoth();
		Metric instance = map.getMetric(NAME);
		Metric newDefinition = definition(NAME);

		map.updateMetricValue(NAME, newDefinition, null);

		assertSame(map.getTemplateDefinition(NAME), newDefinition);
		assertSame(map.getMetric(NAME), instance, "the instance is untouched");
		assertEquals(map.getMetricCount(), Integer.valueOf(2), "nothing was added as a Tag");
	}

	private SparkplugBPayloadMap mapWithBoth() throws Exception {
		SparkplugBPayloadMap map = new SparkplugBPayloadMap();
		map.addMetric(instance(NAME, NAME));
		map.addMetric(definition(NAME));
		return map;
	}

	private static Metric definition(String name) throws Exception {
		Template template = new TemplateBuilder().definition(true)
				.addMetric(new MetricBuilder("Firmware", MetricDataType.Int32, 0).createMetric()).createTemplate();
		return new MetricBuilder(name, MetricDataType.Template, template).createMetric();
	}

	private static Metric instance(String name, String templateRef) throws Exception {
		Template template = new TemplateBuilder().definition(false).templateRef(templateRef)
				.addMetric(new MetricBuilder("Firmware", MetricDataType.Int32, 7).createMetric()).createTemplate();
		return new MetricBuilder(name, MetricDataType.Template, template).createMetric();
	}
}
