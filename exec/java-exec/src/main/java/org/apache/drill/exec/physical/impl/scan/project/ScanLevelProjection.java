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
package org.apache.drill.exec.physical.impl.scan.project;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.project.ScanOutputColumn.RequestedTableColumn;
import org.apache.drill.exec.physical.impl.scan.project.ScanOutputColumn.WildcardColumn;

import com.google.common.collect.Lists;

/**
 * Parses and analyzes the projection list passed to the scanner. The
 * projection list is per scan, independent of any tables that the
 * scanner might scan. The projection list is then used as input to the
 * per-table projection planning.
 * <p>
 * An annoying aspect of SQL is that the projection list (the list of
 * columns to appear in the output) is specified after the SELECT keyword.
 * In Relational theory, projection is about columns, selection is about
 * rows...
 * <p>
 * Mappings can be based on three primary use cases:
 * <ul>
 * <li><tt>SELECT *</tt>: Project all data source columns, whatever they happen
 * to be. Create columns using names from the data source. The data source
 * also determines the order of columns within the row.</li>
 * <li><tt>SELECT columns</tt>: Similar to SELECT * in that it projects all columns
 * from the data source, in data source order. But, rather than creating
 * individual output columns for each data source column, creates a single
 * column which is an array of Varchars which holds the (text form) of
 * each column as an array element.</li>
 * <li><tt>SELECT a, b, c, ...</tt>: Project a specific set of columns, identified by
 * case-insensitive name. The output row uses the names from the SELECT list,
 * but types from the data source. Columns appear in the row in the order
 * specified by the SELECT.</li>
 * </ul>
 * Names in the SELECT list can reference any of five distinct types of output
 * columns:
 * <ul>
 * <li>Wildcard ("*") column: indicates the place in the projection list to insert
 * the table columns once found in the table projection plan.</li>
 * <li>Data source columns: columns from the underlying table. The table
 * projection planner will determine if the column exists, or must be filled
 * in with a null column.</li>
 * <li>The generic data source columns array: <tt>columns</tt>, or optionally
 * specific members of the <tt>columns</tt> array such as <tt>columns[1]</tt>.</li>
 * <li>Implicit columns: <tt>fqn</tt>, <tt>filename</tt>, <tt>filepath</tt>
 * and <tt>suffix</tt>. These reference
 * parts of the name of the file being scanned.</li>
 * <li>Partition columns: <tt>dir0</tt>, <tt>dir1</tt>, ...: These reference
 * parts of the path name of the file.</li>
 * </ul>
 *
 * @see {@link ImplicitColumnExplorer}, the class from which this class
 * evolved
 */

public class ScanLevelProjection {

  public enum ProjectionType { WILDCARD, COLUMNS_ARRAY, LIST }

  public static final String WILDCARD = "*";

  /**
   * Definition of a column from the SELECT list. This is
   * the column before semantic analysis is done to determine
   * the meaning of the column name.
   */

  public static class RequestedColumn {
    private final SchemaPath column;
    protected ScanOutputColumn resolution;

    public RequestedColumn(SchemaPath col) {
      column = col;
    }

    /**
     * Returns the root name:
     * <ul>
     * <li>a: returns a</li>
     * <li>a.b: returns a</li>
     * <li>a[10]: returns a</li>
     * </ul>
     * @return root name
     */

    public String name() { return column.getRootSegment().getPath(); }

    /**
     * Return the output column to which this input column is projected.
     *
     * @return the corresponding output column
     */

    public ScanOutputColumn resolution() {
      return resolution;
    }

    /**
     * Return if this column is the special wildcard ("*") column which means to
     * project all table columns.
     *
     * @return true if the column is "*"
     */

    public boolean isWildcard() { return name().equals(WILDCARD); }

    public boolean isColumnsArray() {
      return name().equalsIgnoreCase(ColumnsArrayParser.COLUMNS_COL);
    }

    public boolean isArray() {
      return ! column.isSimplePath();
    }

    public int arrayIndex() {
      return column.getRootSegment().getChild().getArraySegment().getIndex();
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder()
        .append("[InputColumn path=\"")
        .append(column)
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

  public static class NullColumnProjection extends ScanOutputColumn {

    public NullColumnProjection(RequestedColumn inCol) {
      super(inCol);
    }

    @Override
    public ColumnType columnType() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    protected void visit(int index, Visitor visitor) {
      // TODO Auto-generated method stub

    }
  }

  public interface ScanProjectionParser {
    void bind(ScanProjectionBuilder builder);
    boolean parse(RequestedColumn inCol);
    void validate();
    void validateColumn(ScanOutputColumn col);
    void build();
  }

  /**
   * Fluent builder for the projection mapping. Accepts the inputs needed to
   * plan a projection, builds the mappings, and constructs the projection
   * mapping object.
   * <p>
   * Builds the per-scan projection plan given a set of projected columns.
   * Determines the output schema, which columns to project from the data
   * source, which are metadata, and so on.
   */

  public static class ScanProjectionBuilder {

    // Input

    protected List<RequestedColumn> projectionList = new ArrayList<>();
    protected List<ScanProjectionParser> parsers = new ArrayList<>();

    // Output

    protected List<ScanOutputColumn> outputCols = new ArrayList<>();
    protected List<String> tableColNames = new ArrayList<>();
    protected ProjectionType projectionType;
    protected RequestedColumn wildcardColumn;

    public ScanProjectionBuilder() {}

    public void addParser(ScanProjectionParser parser) {
      parsers.add(parser);
      parser.bind(this);
    }

    /**
     * Indicate a SELECT * query.
     *
     * @return this builder
     */
    public ScanProjectionBuilder projectAll() {
      return projectedCols(Lists.newArrayList(new SchemaPath[] {SchemaPath.getSimplePath(WILDCARD)}));
    }

    /**
     * Specify the set of columns in the SELECT list. Since the column list
     * comes from the query planner, assumes that the planner has checked
     * the list for syntax and uniqueness.
     *
     * @param queryCols list of columns in the SELECT list in SELECT list order
     * @return this builder
     */
    public ScanProjectionBuilder projectedCols(List<SchemaPath> queryCols) {
      assert this.projectionList.isEmpty();
      this.projectionList = new ArrayList<>();
      for (SchemaPath col : queryCols) {
        RequestedColumn sCol = new RequestedColumn(col);
        this.projectionList.add(sCol);
      }
      return this;
    }

    public void requestColumns(List<RequestedColumn> queryCols) {
      assert this.projectionList.isEmpty();
      this.projectionList.addAll(queryCols);
    }

    /**
     * Perform projection planning and return the final projection plan.
     * @return the finalized projection plan
     */

    public ScanLevelProjection build() {
      projectionType = ProjectionType.LIST;
      for (RequestedColumn inCol : projectionList) {
        mapColumn(inCol);
      }
      verify();
      for (ScanProjectionParser parser : parsers) {
        parser.build();
      }
      return new ScanLevelProjection(this);
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

    private void mapColumn(RequestedColumn inCol) {
      for (ScanProjectionParser parser : parsers) {
        if (parser.parse(inCol)) {
          return;
        }
      }
      if (inCol.isWildcard()) {

        // Star column: this is a SELECT * query.

        mapWildcardColumn(inCol);
        return;
      }
      // This is a desired table column.

      RequestedTableColumn tableCol = RequestedTableColumn.fromSelect(inCol);
      inCol.resolution = tableCol;
      outputCols.add(tableCol);
      tableColNames.add(tableCol.name());
    }

    public void setProjectionType(ProjectionType type) {
      projectionType = type;
    }

    public void addProjectedColumn(ScanOutputColumn outCol) {
      outputCols.add(outCol);
    }

    private void mapWildcardColumn(RequestedColumn inCol) {
      if (wildcardColumn != null) {
        throw new IllegalArgumentException("Duplicate * entry in project list");
      }
      projectionType = ProjectionType.WILDCARD;
      wildcardColumn = inCol;

      // Put the wildcard column into the projection list as a placeholder to be filled
      // in later with actual table columns.

      inCol.resolution = WildcardColumn.fromSelect(inCol);
      outputCols.add(inCol.resolution);
    }

    private void verify() {
      for (ScanProjectionParser parser : parsers) {
        parser.validate();
      }
      for (ScanOutputColumn outCol : outputCols) {
        for (ScanProjectionParser parser : parsers) {
          parser.validateColumn(outCol);
        }
        switch (outCol.columnType()) {
        case TABLE:
          if (hasWildcard()) {
            throw new IllegalArgumentException("Cannot select table columns and `*` together");
          }
          break;
        default:
          break;
        }
      }
    }

    public boolean hasWildcard() {
      return wildcardColumn != null;
    }
  }

  private final ProjectionType projectType;
  private final List<RequestedColumn> requestedCols;
  private final List<ScanOutputColumn> outputCols;
  private final List<String> tableColNames;

  public ScanLevelProjection(ScanProjectionBuilder builder) {
    projectType = builder.projectionType;
     requestedCols = builder.projectionList;
    outputCols = builder.outputCols;
    tableColNames = builder.tableColNames;
  }

  /**
   * Return whether this is a SELECT * query
   * @return true if this is a SELECT * query
   */

  public boolean isProjectAll() { return projectType == ProjectionType.WILDCARD; }

  public ProjectionType projectType() { return projectType; }

  /**
   * Return the set of columns from the SELECT list
   * @return the SELECT list columns, in SELECT list order
   */

  public List<RequestedColumn> requestedCols() { return requestedCols; }

  /**
   * The entire set of output columns, in output order. Output order is
   * that specified in the SELECT (for an explicit list of columns) or
   * table order (for SELECT * queries).
   * @return the set of output columns in output order
   */

  public List<ScanOutputColumn> outputCols() { return outputCols; }

  public List<String> tableColNames() { return tableColNames; }

  public static MajorType nullType() {
    return MajorType.newBuilder()
        .setMinorType(MinorType.NULL)
        .setMode(DataMode.OPTIONAL)
        .build();
  }
}
