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
package org.eclipse.tahu.model;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.eclipse.tahu.message.model.MetricDataType;

public class MetricDataTypeMap {

	private final Map<String, MetricDataType> nameDataTypeMap;
	private final Map<Long, MetricDataType> aliasDataTypeMap;
	private final Map<Long, String> aliasNameMap;

	public MetricDataTypeMap() {
		nameDataTypeMap = new ConcurrentHashMap<>();
		aliasDataTypeMap = new ConcurrentHashMap<>();
		aliasNameMap = new ConcurrentHashMap<>();
	}

	public void addMetricDataType(String metricName, MetricDataType metricDataType) {
		nameDataTypeMap.put(metricName, metricDataType);
	}

	public void addMetricDataType(Long alias, MetricDataType metricDataType) {
		aliasDataTypeMap.put(alias, metricDataType);
	}

	public void addMetricName(Long alias, String metricName) {
		aliasNameMap.put(alias, metricName);
	}

	public String getMetricName(Long alias) {
		return aliasNameMap.get(alias);
	}

	public MetricDataType getMetricDataType(String metricName) {
		return nameDataTypeMap.get(metricName);
	}

	public MetricDataType getMetricDataType(Long alias) {
		return aliasDataTypeMap.get(alias);
	}

	public boolean isEmpty() {
		if (nameDataTypeMap.isEmpty() && aliasDataTypeMap.isEmpty()) {
			return true;
		} else {
			return false;
		}
	}

	public void clear() {
		nameDataTypeMap.clear();
		aliasNameMap.clear();
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("MetricDataTypeMap [nameDataTypeMap=");
		builder.append(nameDataTypeMap);
		builder.append(", aliasDataTypeMap=");
		builder.append(aliasDataTypeMap);
		builder.append("]");
		return builder.toString();
	}
}
