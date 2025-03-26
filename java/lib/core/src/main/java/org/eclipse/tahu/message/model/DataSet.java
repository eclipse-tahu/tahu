/********************************************************************************
 * Copyright (c) 2014-2024 Cirrus Link Solutions and others
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

import org.apache.commons.compress.utils.Lists;
import org.eclipse.tahu.SparkplugException;
import org.eclipse.tahu.exception.TahuErrorCode;
import org.eclipse.tahu.exception.TahuException;
import org.eclipse.tahu.json.DataSetDeserializer;
import org.eclipse.tahu.message.model.Row.RowBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * A data set that represents a table of data.
 */
@JsonDeserialize(
		using = DataSetDeserializer.class)
public class DataSet {

	private static final Logger logger = LoggerFactory.getLogger(DataSet.class.getName());

	/**
	 * The number of columns
	 */
	@JsonProperty("numberOfColumns")
	private long numOfColumns;

	/**
	 * A list containing the names of each column
	 */
	@JsonProperty("columnNames")
	private List<String> columnNames;

	/**
	 * A list containing the data types of each column
	 */
	@JsonProperty("types")
	private List<DataSetDataType> types;

	/**
	 * A list containing the rows in the data set
	 */
	private List<Row> rows;

	/**
	 * Default Constructor
	 */
	public DataSet() {
	}

	/**
	 * Constructor
	 *
	 * @param numOfColumns the number of columns in the {@link DataSet}
	 * @param columnNames a {@link List} of column names in the {@link DataSet}
	 * @param types a {@link List} of {@link DataSetDataTypes}s for the columns
	 * @param rows a {@link List} of {@link Row}s in the {@link DataSet}
	 */
	public DataSet(long numOfColumns, List<String> columnNames, List<DataSetDataType> types, List<Row> rows) {
		this.numOfColumns = numOfColumns;
		this.columnNames = columnNames;
		this.types = types;
		this.rows = rows;
	}

	/**
	 * Copy Constructor
	 *
	 * @param dataSet the {@link DataSet} to copy
	 */
	public DataSet(DataSet dataSet) throws TahuException {
		this.numOfColumns = dataSet.getNumOfColumns();
		if (numOfColumns > 0) {
			if (dataSet.getColumnNames() == null || dataSet.getTypes() == null
					|| dataSet.getColumnNames().size() != dataSet.getTypes().size()) {
				throw new TahuException(TahuErrorCode.INVALID_ARGUMENT, "Invalid DataSet to copy " + dataSet);
			}
			this.columnNames = new ArrayList<>();
			for (String columnName : dataSet.getColumnNames()) {
				this.columnNames.add(columnName);
			}
			this.types = new ArrayList<>();
			for (DataSetDataType type : dataSet.getTypes()) {
				this.types.add(type);
			}
			this.rows = new ArrayList<>();
			if (dataSet.getRows() != null) {
				for (Row row : dataSet.getRows()) {
					this.rows.add(new Row(row));
				}
			}
		}
	}

	/**
	 * Returns the number of columns in the {@link DataSet}
	 *
	 * @return the number of columns in the {@link DataSet}
	 */
	public long getNumOfColumns() {
		return numOfColumns;
	}

	/**
	 * Sets the number of columns in the {@link DataSet}
	 *
	 * @param numOfColumns the number of columns to set in the {@link DataSet}
	 */
	public void setNumOfColumns(long numOfColumns) {
		this.numOfColumns = numOfColumns;
	}

	/**
	 * Gets a {@link List} of the column names in the {@link DataSet}
	 *
	 * @return a {@link List} of the column names in the {@link DataSet}
	 */
	public List<String> getColumnNames() {
		return columnNames;
	}

	/**
	 * Sets a {@link List} of column names in the {@link DataSet}
	 *
	 * @param columnNames a {@link List} of column names to set in the {@link DataSet}
	 */
	public void setColumnNames(List<String> columnNames) {
		this.columnNames = columnNames;
	}

	/**
	 * Adds a column name to the {@link List} of column names in the {@link DataSet}
	 *
	 * @param columnName the column name to add to the {@link List} of column names in the {@link DataSet}
	 */
	public void addColumnName(String columnName) {
		this.columnNames.add(columnName);
	}

	/**
	 * Gets a {@link List} of {@link Row}s for the {@link DataSet}
	 *
	 * @return a {@link List} of {@link Row}s for the {@link DataSet}
	 */
	public List<Row> getRows() {
		return rows;
	}

	/**
	 * Gets a {@link List} of {@link List}s of {@link Object}s representing the rows in the {@link DataSet}
	 *
	 * @return a {@link List} of {@link List}s of {@link Object}s representing the rows in the {@link DataSet}
	 */
	@JsonGetter("rows")
	public List<List<Object>> getRowsAsLists() {
		if (getRows() == null) {
			return Lists.newArrayList();
		} else {
			List<List<Object>> list = new ArrayList<List<Object>>(getRows().size());
			for (Row row : getRows()) {
				list.add(Row.toValues(row));
			}
			return list;
		}
	}

	/**
	 * Adds a {@link Row} to the {@link DataSet}
	 *
	 * @param row a {@link Row} to add to the {@link DataSet}
	 */
	public void addRow(Row row) {
		this.rows.add(row);
	}

	/**
	 * Adds a {@link Row} to the {@link DataSet} at a specified index
	 *
	 * @param index the index to add the {@link Row} in the {@link List} of {@link Row}s
	 * @param row the {@link Row} to add
	 */
	public void addRow(int index, Row row) {
		this.rows.add(index, row);
	}

	/**
	 * Removes a {@link Row} at a specified index
	 *
	 * @param index the index to use when removing the {@link Row}
	 *
	 * @return the removed {@link Row}
	 */
	public Row removeRow(int index) {
		return rows.remove(index);
	}

	/**
	 * Removes a {@link Row} by equality to another {@link Row}
	 *
	 * @param row the {@link Row} to remove
	 *
	 * @return true if the {@link Row} was removed, otherwise false
	 */
	public boolean removeRow(Row row) {
		return rows.remove(row);
	}

	/**
	 * Sets the {@link List} of {@link Row}s to set for the {@link DataSet}
	 *
	 * @param rows the {@link List} of {@link Row}s to set for the {@link DataSet}
	 */
	public void setRows(List<Row> rows) {
		this.rows = rows;
	}

	/**
	 * Gets a {@link List} of {@link DataSetDataType}s for the {@link DataSet}
	 *
	 * @return a {@link List} of {@link DataSetDataType}s for the {@link DataSet}
	 */
	public List<DataSetDataType> getTypes() {
		return types;
	}

	/**
	 * Sets the {@link List} of {@link DataSetDataType}s for the {@link DataSet}
	 *
	 * @param types the {@link List} of {@link DataSetDataType}s to set for the {@link DataSet}
	 */
	public void setTypes(List<DataSetDataType> types) {
		this.types = types;
	}

	/**
	 * Adds a {@link DataSetDataType} to the end of the {@link List} of {@link DataSetDataType}s in the {@link DataSet}
	 *
	 * @param type the {@link DataSetDateType} to add to the end of the {@link DataSet}
	 */
	public void addType(DataSetDataType type) {
		this.types.add(type);
	}

	/**
	 * Adds a {@link DataSetDataType} to the at the specified index to the {@link List} of {@link DataSetDataType}s in
	 * the {@link DataSet}
	 *
	 * @param index the index at which to add the new {@link DataSetDataType}
	 * @param type the {@link DataSetDateType} to add to the {@link DataSet}
	 */
	public void addType(int index, DataSetDataType type) {
		this.types.add(index, type);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("DataSet [numOfColumns=");
		builder.append(numOfColumns);
		builder.append(", columnNames=");
		builder.append(columnNames);
		builder.append(", types=");
		if (types != null) {
			builder.append("[");
			for (int i = 0; i < types.size(); i++) {
				builder.append(types.get(i).getType());
				if (i + 1 != types.size()) {
					builder.append(", ");
				}
			}
			builder.append("]");
		} else {
			builder.append("null");
		}
		builder.append(", rows=");
		builder.append(rows);
		builder.append("]");
		return builder.toString();
	}

	/**
	 * A builder for creating a {@link DataSet} instance.
	 */
	public static class DataSetBuilder {

		private long numOfColumns;
		private List<String> columnNames;
		private List<DataSetDataType> types;
		private List<Row> rows;

		public DataSetBuilder(long numOfColumns) {
			this.numOfColumns = numOfColumns;
			this.columnNames = new ArrayList<String>();
			this.types = new ArrayList<DataSetDataType>();
			this.rows = new ArrayList<Row>();
		}

		public DataSetBuilder(DataSet dataSet) {
			this.numOfColumns = dataSet.getNumOfColumns();
			if (dataSet.getColumnNames() == null) {
				this.columnNames = Lists.newArrayList();
			} else {
				this.columnNames = new ArrayList<String>(dataSet.getColumnNames());
			}
			if (dataSet.getTypes() == null) {
				this.types = Lists.newArrayList();
			} else {
				this.types = new ArrayList<DataSetDataType>(dataSet.getTypes());
			}
			if (dataSet.getRows() == null) {
				this.rows = Lists.newArrayList();
			} else {
				this.rows = new ArrayList<Row>(dataSet.getRows().size());
				for (Row row : dataSet.getRows()) {
					rows.add(new RowBuilder(row).createRow());
				}
			}
		}

		public DataSetBuilder addColumnNames(Collection<String> columnNames) {
			this.columnNames.addAll(columnNames);
			return this;
		}

		public DataSetBuilder addColumnName(String columnName) {
			this.columnNames.add(columnName);
			return this;
		}

		public DataSetBuilder addType(DataSetDataType type) {
			this.types.add(type);
			return this;
		}

		public DataSetBuilder addTypes(Collection<DataSetDataType> types) {
			this.types.addAll(types);
			return this;
		}

		public DataSetBuilder addRow(Row row) {
			this.rows.add(row);
			return this;
		}

		public DataSetBuilder addRows(Collection<Row> rows) {
			this.rows.addAll(rows);
			return this;
		}

		public DataSet createDataSet() throws SparkplugException {
			logger.trace("Number of columns: " + numOfColumns);
			for (String columnName : columnNames) {
				logger.trace("\tcolumnName: " + columnName);
			}
			for (DataSetDataType type : types) {
				logger.trace("\ttypes: " + type);
			}
			for (Row row : rows) {
				logger.trace("\t\trow: " + row);
			}

			validate();
			return new DataSet(numOfColumns, columnNames, types, rows);
		}

		public void validate() throws SparkplugException {
			if (columnNames.size() != numOfColumns) {
				throw new SparkplugException("Invalid number of columns in data set column names: " + columnNames.size()
						+ " vs expected " + numOfColumns);
			}
			if (types.size() != numOfColumns) {
				throw new SparkplugException("Invalid number of columns in data set types: " + types.size()
						+ " vs expected: " + numOfColumns);
			}
			for (int i = 0; i < types.size(); i++) {
				for (Row row : rows) {
					List<Value<?>> values = row.getValues();
					if (values.size() != numOfColumns) {
						throw new SparkplugException("Invalid number of columns in data set row: " + values.size()
								+ " vs expected: " + numOfColumns);
					}
					types.get(i).checkType(row.getValues().get(i).getValue());
				}
			}
		}
	}
}
