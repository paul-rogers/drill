/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl.scan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.impl.scan.SchemaNegotiator.ColumnType;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.store.ImplicitColumnExplorer;
import org.apache.drill.exec.store.ImplicitColumnExplorer.ImplicitFileColumns;

import com.google.common.collect.Lists;

/**
 * Parses and analyzes the selection list passed to the scanner. The
 * selection list is per scan, independent of any tables that the
 * scanner might scan. The selection list is then used as input to the
 * per-table projection planning.
 * <p>
 * Mappings can be based on three primary use cases:
 * <ul>
 * <li>SELECT *: Select all data source columns, whatever they happen
 * to be. Create columns using names from the data source. The data source
 * also determines the order of columns within the row.</li>
 * <li>SELECT columns: Similar to SELECT * in that it selects all columns
 * from the data source, in data source order. But, rather than creating
 * individual output columns for each data source column, creates a single
 * column which is an array of Varchars which holds the (text form) of
 * each column as an array element.</li>
 * <li>SELECT a, b, c, ...: Select a specific set of columns, identified by
 * case-insensitive name. The output row uses the names from the SELECT list,
 * but types from the data source. Columns appear in the row in the order
 * specified by the SELECT.</li>
 * </ul>
 * Names in the SELECT list can reference any of four distinct types of output
 * columns:
 * <ul>
 * <li>Wildcard ("*") column: indictes the place in the select list to insert
 * the table columns once found in the table projection plan.</li>
 * <li>Data source columns: columns from the underlying table. The table
 * projection planner will determine if the column exists, or must be filled
 * in with a null column.</i>
 * <li>Implicit columns: fqn, filename, filepath and suffix. These reference
 * parts of the name of the file being scanned.</li>
 * <li>Partition columns: dir0, dir1, ...: These reference parts of the path
 * name of the file.</li>
 * </ul>
 *
 * @see {@link ImplicitColumnExplorer}, the class from which this class
 * evolved
 */

public class QuerySelectionPlan {

  public enum SelectType { WILDCARD, COLUMNS_ARRAY, LIST }

  public static final String WILDCARD = "*";
  public static final String COLUMNS_ARRAY_NAME = "columns";

  /**
   * Definition of a column from the SELECT list. This is
   * the column before semantic analysis is done to determine
   * the meaning of the column name.
   */

  public static class SelectColumn {
    private final String name;
    @SuppressWarnings("unused")
    private final ColumnType typeHint;
    protected OutputColumn resolution;

    public SelectColumn(SchemaPath col) {
      this(col, null);
    }

    public SelectColumn(SchemaPath col, ColumnType hint) {
      this.name = col.getAsUnescapedPath();
      typeHint = hint;
    }

    public String name() { return name; }

    /**
     * Return the output column to which this input column is projected.
     *
     * @return the corresponding output column
     */

    public OutputColumn resolution() {
      return resolution;
    }

    /**
     * Return if this column is the special "*" column which means to select
     * all table columns.
     *
     * @return true if the column is "*"
     */

    public boolean isWildcard() { return name.equals(WILDCARD); }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder()
        .append("[InputColumn name=\"")
        .append(name)
        .append("\", resolution=");
      if (resolution == null) {
        buf.append("null");
      } else {
        buf.append(resolution.toString());
      }
      buf.append("]");
      return buf.toString();
    }
  }

  /**
   * Definition of a file metadata (AKA "implicit") column for this query.
   * Provides the static definition, along
   * with the name set for the implicit column in the session options for the query.
   */

  public static class FileMetadataColumnDefn {
    public final ImplicitFileColumns defn;
    public final String colName;

    public FileMetadataColumnDefn(String colName, ImplicitFileColumns defn) {
      this.colName = colName;
      this.defn = defn;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder()
        .append("[FileInfoColumnDefn name=\"")
        .append(colName)
        .append("\", defn=")
        .append(defn)
        .append("]");
      return buf.toString();
    }
  }

  /**
   * Fluent builder for the projection mapping. Accepts the inputs needed to
   * plan a projection, builds the mappings, and constructs the selection
   * mapping object.
   * <p>
   * Builds the per-scan projection plan given a set of selected columns.
   * Determines the output schema, which columns to project from the data
   * source, which are metadata, and so on.
   */

  public static class Builder {

    // Config

    private final String partitionDesignator;
    private final Pattern partitionPattern;
    private List<FileMetadataColumnDefn> implicitColDefns = new ArrayList<>();;
    private Map<String, FileMetadataColumnDefn> fileMetadataColIndex = CaseInsensitiveMap.newHashMap();
    private boolean useLegacyStarPlan = true;

    // Input

    protected List<SelectColumn> queryCols = new ArrayList<>();

    // Output

    protected List<OutputColumn> outputCols = new ArrayList<>();
    protected OutputColumn.ColumnsArrayColumn columnsArrayCol;
    protected SelectType selectType;
    protected SelectColumn starColumn;
    protected boolean hasMetadata;

    public Builder(OptionSet optionManager) {
      partitionDesignator = optionManager.getOption(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL_VALIDATOR);
      partitionPattern = Pattern.compile(partitionDesignator + "(\\d+)", Pattern.CASE_INSENSITIVE);
      for (ImplicitFileColumns e : ImplicitFileColumns.values()) {
        OptionValue optionValue = optionManager.getOption(e.optionName());
        if (optionValue != null) {
          FileMetadataColumnDefn defn = new FileMetadataColumnDefn(optionValue.string_val, e);
          implicitColDefns.add(defn);
          fileMetadataColIndex.put(defn.colName, defn);
        }
      }
    }

    /**
     * Indicate a SELECT * query.
     *
     * @return this builder
     */
    public Builder selectAll() {
      return queryCols(Lists.newArrayList(new SchemaPath[] {SchemaPath.getSimplePath(WILDCARD)}));
    }

    /**
     * Specifies whether to plan based on the legacy meaning of "*". See
     * <a href="https://issues.apache.org/jira/browse/DRILL-5542">DRILL-5542</a>.
     * If true, then the star column <i>includes</i> implicit and partition
     * columns. If false, then star matches <i>only</i> table columns.
     * @param flag true to use the legacy plan, false to use the revised
     * semantics
     * @return this builder
     */

    public Builder useLegacyStarPlan(boolean flag) {
      useLegacyStarPlan = flag;
      return this;
    }

    /**
     * Specify the set of columns in the SELECT list. Since the column list
     * comes from the query planner, assumes that the planner has checked
     * the list for syntax and uniqueness.
     *
     * @param queryCols list of columns in the SELECT list in SELECT list order
     * @return this builder
     */
    public Builder queryCols(List<SchemaPath> queryCols) {
      assert this.queryCols.isEmpty();
      this.queryCols = new ArrayList<>();
      for (SchemaPath col : queryCols) {
        SelectColumn sCol = new SelectColumn(col);
        this.queryCols.add(sCol);
      }
      return this;
    }

    public void selection(List<SelectColumn> queryCols) {
      assert this.queryCols.isEmpty();
      this.queryCols.addAll(queryCols);
    }

    /**
     * Perform projection planning and return the final projection plan.
     * @return the finalized projection plan
     */

    public QuerySelectionPlan build() {
      selectType = SelectType.LIST;
      for (SelectColumn inCol : queryCols) {
        mapColumn(inCol);
      }
      verify();
      return new QuerySelectionPlan(this);
    }

    /**
     * Map the column into one of five categories.
     * <ol>
     * <li>Star column (to designate SELECT *)</li>
     * <li>Partition file column (dir0, dir1, etc.)</li>
     * <li>Implicit column (fqn, filepath, filename, suffix)</li>
     * <li>Special <tt>columns</tt> column which holds all columns as
     * an array.</li>
     * <li>Table column. The actual match against the table schema
     * is done later.</li>
     * </ol>
     *
     * @param inCol the SELECT column
     */

    private void mapColumn(SelectColumn inCol) {
      if (inCol.isWildcard()) {

        // Star column: this is a SELECT * query.

        mapWildcardColumn(inCol);
        return;
      }
      Matcher m = partitionPattern.matcher(inCol.name());
      if (m.matches()) {

        // Partition column

        OutputColumn.PartitionColumn outCol = new OutputColumn.PartitionColumn(inCol, Integer.parseInt(m.group(1)));
        inCol.resolution = outCol;
        outputCols.add(outCol);
        hasMetadata = true;
        return;
      }

      FileMetadataColumnDefn iCol = fileMetadataColIndex.get(inCol.name());
      if (iCol != null) {

        // File metadata (implicit) column

        OutputColumn.FileMetadataColumn outCol = new OutputColumn.FileMetadataColumn(inCol, iCol);
        inCol.resolution = outCol;
        outputCols.add(outCol);
        hasMetadata = true;
        return;
      }

      if (inCol.name().equalsIgnoreCase(COLUMNS_ARRAY_NAME)) {

        // Special `columns` array column.

        mapColumnsArrayColumn(inCol);
        return;
      }

      // This is a desired table column.

      OutputColumn.ExpectedTableColumn tableCol = new OutputColumn.ExpectedTableColumn(inCol);
      inCol.resolution = tableCol;
      outputCols.add(tableCol);
    }

    private void mapWildcardColumn(SelectColumn inCol) {
      if (starColumn != null) {
        throw new IllegalArgumentException("Duplicate * entry in select list");
      }
      selectType = SelectType.WILDCARD;
      starColumn = inCol;

      // Put the wildcard column into the projection list as a placeholder to be filled
      // in later with actual table columns.

      inCol.resolution = new OutputColumn.WildcardColumn(inCol, useLegacyStarPlan);
      outputCols.add(inCol.resolution);
      if (useLegacyStarPlan) {
        hasMetadata = true;
        for (FileMetadataColumnDefn iCol : implicitColDefns) {
          OutputColumn.FileMetadataColumn outCol = new OutputColumn.FileMetadataColumn(starColumn, iCol);
          outputCols.add(outCol);
        }
      }
    }

    private void mapColumnsArrayColumn(SelectColumn inCol) {

      if (columnsArrayCol != null) {
        throw new IllegalArgumentException("Duplicate columns[] column");
      }

      selectType = SelectType.COLUMNS_ARRAY;
      columnsArrayCol = new OutputColumn.ColumnsArrayColumn(inCol);
      outputCols.add(columnsArrayCol);
      inCol.resolution = columnsArrayCol;
    }

    private void verify() {
      if (starColumn != null && columnsArrayCol != null) {
        throw new IllegalArgumentException("Cannot select columns[] and `*` together");
      }
      for (OutputColumn outCol : outputCols) {
        if (outCol.columnType() == OutputColumn.ColumnType.TABLE) {
          if (starColumn != null) {
            throw new IllegalArgumentException("Cannot select table columns and `*` together");
          }
          if (columnsArrayCol != null) {
            throw new IllegalArgumentException("Cannot select columns[] and other table columns: " + outCol.name());
          }
        }
      }
    }
  }

  private final SelectType selectType;
  private final boolean hasMetadata;
  private final boolean useLegacyStarPlan;
  private final List<SelectColumn> queryCols;
  private final List<OutputColumn> outputCols;

  public QuerySelectionPlan(Builder builder) {
    selectType = builder.selectType;
    hasMetadata = builder.hasMetadata;
    useLegacyStarPlan = builder.useLegacyStarPlan;
    queryCols = builder.queryCols;
    outputCols = builder.outputCols;
  }

  /**
   * Return whether this is a SELECT * query
   * @return true if this is a SELECT * query
   */

  public boolean isSelectAll() { return selectType == SelectType.WILDCARD; }

  public SelectType selectType() { return selectType; }

  /**
   * Return the set of columns from the SELECT list
   * @return the SELECT list columns, in SELECT list order,
   * or null if this is a SELECT * query
   */

  public List<SelectColumn> queryCols() { return queryCols; }

  public boolean hasMetadata() { return hasMetadata; }

  /**
   * The entire set of output columns, in output order. Output order is
   * that specified in the SELECT (for an explicit list of columns) or
   * table order (for SELECT * queries).
   * @return the set of output columns in output order
   */

  public List<OutputColumn> outputCols() { return outputCols; }

  public boolean useLegacyWildcardPartition() { return useLegacyStarPlan; }

  // TODO: Test allowing * (for table) along with metadata columns
}
