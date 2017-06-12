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
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.ColumnsArrayColumn;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.FileMetadataColumn;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.PartitionColumn;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.RequestedTableColumn;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.WildcardColumn;
import org.apache.drill.exec.physical.impl.scan.SchemaNegotiator.ColumnType;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.store.ImplicitColumnExplorer;
import org.apache.drill.exec.store.ImplicitColumnExplorer.ImplicitFileColumns;

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
 * <li>SELECT *: Project all data source columns, whatever they happen
 * to be. Create columns using names from the data source. The data source
 * also determines the order of columns within the row.</li>
 * <li>SELECT columns: Similar to SELECT * in that it projects all columns
 * from the data source, in data source order. But, rather than creating
 * individual output columns for each data source column, creates a single
 * column which is an array of Varchars which holds the (text form) of
 * each column as an array element.</li>
 * <li>SELECT a, b, c, ...: Project a specific set of columns, identified by
 * case-insensitive name. The output row uses the names from the SELECT list,
 * but types from the data source. Columns appear in the row in the order
 * specified by the SELECT.</li>
 * </ul>
 * Names in the SELECT list can reference any of four distinct types of output
 * columns:
 * <ul>
 * <li>Wildcard ("*") column: indicates the place in the projection list to insert
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

public class ScanProjectionDefn {

  public enum ProjectionType { WILDCARD, COLUMNS_ARRAY, LIST }

  public static final String WILDCARD = "*";
  public static final String COLUMNS_ARRAY_NAME = "columns";

  /**
   * Definition of a column from the SELECT list. This is
   * the column before semantic analysis is done to determine
   * the meaning of the column name.
   */

  public static class RequestedColumn {
    private final String name;
    @SuppressWarnings("unused")
    private final ColumnType typeHint;
    protected ScanOutputColumn resolution;

    public RequestedColumn(SchemaPath col) {
      this(col, null);
    }

    public RequestedColumn(SchemaPath col, ColumnType hint) {
      this.name = col.getAsUnescapedPath();
      typeHint = hint;
    }

    public String name() { return name; }

    /**
     * Return the output column to which this input column is projected.
     *
     * @return the corresponding output column
     */

    public ScanOutputColumn resolution() {
      return resolution;
    }

    /**
     * Return if this column is the special "*" column which means to project
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

    public String colName() { return colName; }

    public MajorType dataType() {
      return MajorType.newBuilder()
          .setMinorType(MinorType.VARCHAR)
          .setMode(DataMode.REQUIRED)
          .build();
    }
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

  public static class Builder {

    // Config

    private final String partitionDesignator;
    private final Pattern partitionPattern;
    private List<FileMetadataColumnDefn> implicitColDefns = new ArrayList<>();;
    private Map<String, FileMetadataColumnDefn> fileMetadataColIndex = CaseInsensitiveMap.newHashMap();
    private boolean useLegacyWildcardExpansion = true;
    private MajorType columnsArrayType;

    // Input

    protected List<RequestedColumn> queryCols = new ArrayList<>();

    // Output

    protected List<ScanOutputColumn> outputCols = new ArrayList<>();
    protected List<String> tableColNames = new ArrayList<>();
    protected ColumnsArrayColumn columnsArrayCol;
    protected ProjectionType projectionType;
    protected RequestedColumn wildcardColumn;
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
    public Builder projectAll() {
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

    public Builder useLegacyWildcardExpansion(boolean flag) {
      useLegacyWildcardExpansion = flag;
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
        RequestedColumn sCol = new RequestedColumn(col);
        this.queryCols.add(sCol);
      }
      return this;
    }

    public void requestColumns(List<RequestedColumn> queryCols) {
      assert this.queryCols.isEmpty();
      this.queryCols.addAll(queryCols);
    }

    public void columnsArrayType(MinorType type) {
      columnsArrayType = MajorType.newBuilder()
          .setMinorType(type)
          .setMode(DataMode.REPEATED)
          .build();
    }

    /**
     * Perform projection planning and return the final projection plan.
     * @return the finalized projection plan
     */

    public ScanProjectionDefn build() {
      projectionType = ProjectionType.LIST;
      for (RequestedColumn inCol : queryCols) {
        mapColumn(inCol);
      }
      verify();
      return new ScanProjectionDefn(this);
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
      if (inCol.isWildcard()) {

        // Star column: this is a SELECT * query.

        mapWildcardColumn(inCol);
        return;
      }
      Matcher m = partitionPattern.matcher(inCol.name());
      if (m.matches()) {

        // Partition column

        PartitionColumn outCol = PartitionColumn.fromSelect(inCol, Integer.parseInt(m.group(1)));
        inCol.resolution = outCol;
        outputCols.add(outCol);
        hasMetadata = true;
        return;
      }

      FileMetadataColumnDefn iCol = fileMetadataColIndex.get(inCol.name());
      if (iCol != null) {

        // File metadata (implicit) column

        FileMetadataColumn outCol = FileMetadataColumn.fromSelect(inCol, iCol);
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

      RequestedTableColumn tableCol = RequestedTableColumn.fromSelect(inCol);
      inCol.resolution = tableCol;
      outputCols.add(tableCol);
      tableColNames.add(tableCol.name());
    }

    private void mapWildcardColumn(RequestedColumn inCol) {
      if (wildcardColumn != null) {
        throw new IllegalArgumentException("Duplicate * entry in select list");
      }
      projectionType = ProjectionType.WILDCARD;
      wildcardColumn = inCol;

      // Put the wildcard column into the projection list as a placeholder to be filled
      // in later with actual table columns.

      inCol.resolution = WildcardColumn.fromSelect(inCol);
      if (useLegacyWildcardExpansion) {

        // Old-style wildcard handling inserts all metadata coluns in
        // the scanner, removes them in Project.
        // Fill in the file metadata columns. Can do here because the
        // set is constant across all files.

        hasMetadata = true;
      }
      outputCols.add(inCol.resolution);
    }

    private void mapColumnsArrayColumn(RequestedColumn inCol) {

      if (columnsArrayCol != null) {
        throw new IllegalArgumentException("Duplicate columns[] column");
      }

      projectionType = ProjectionType.COLUMNS_ARRAY;
      columnsArrayCol = ColumnsArrayColumn.fromSelect(inCol, columnsArrayType());
      outputCols.add(columnsArrayCol);
      inCol.resolution = columnsArrayCol;
    }

    private void verify() {
      if (wildcardColumn != null && columnsArrayCol != null) {
        throw new IllegalArgumentException("Cannot select columns[] and `*` together");
      }
      for (ScanOutputColumn outCol : outputCols) {
        switch (outCol.columnType()) {
        case TABLE:
          if (wildcardColumn != null) {
            throw new IllegalArgumentException("Cannot select table columns and `*` together");
          }
          if (columnsArrayCol != null) {
            throw new IllegalArgumentException("Cannot select columns[] and other table columns: " + outCol.name());
          }
          break;
        case FILE_METADATA:
          if (wildcardColumn != null  &&  useLegacyWildcardExpansion) {
            throw new IllegalArgumentException("Cannot select file metadata columns and `*` together");
          }
          break;
        case PARTITION:
          if (wildcardColumn != null  &&  useLegacyWildcardExpansion) {
            throw new IllegalArgumentException("Cannot select partitions and `*` together");
          }
          break;
        default:
          break;
        }
      }
    }

    public MajorType columnsArrayType() {
      if (columnsArrayType == null) {
        columnsArrayType = MajorType.newBuilder()
            .setMinorType(MinorType.VARCHAR)
            .setMode(DataMode.REPEATED)
            .build();
      }
      return columnsArrayType;
    }
  }

  private final String partitionDesignator;
  private final List<FileMetadataColumnDefn> fileMetadataColDefns;
  private final ProjectionType projectType;
  private final boolean hasMetadata;
  private final boolean useLegacyStarPlan;
  private final List<RequestedColumn> requestedCols;
  private final List<ScanOutputColumn> outputCols;
  private final List<String> tableColNames;
  private final MajorType columnsArrayType;

  public ScanProjectionDefn(Builder builder) {
    partitionDesignator = builder.partitionDesignator;
    fileMetadataColDefns = builder.implicitColDefns;
    projectType = builder.projectionType;
    hasMetadata = builder.hasMetadata;
    useLegacyStarPlan = builder.useLegacyWildcardExpansion;
    requestedCols = builder.queryCols;
    outputCols = builder.outputCols;
    tableColNames = builder.tableColNames;
    columnsArrayType = builder.columnsArrayType;
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

  public boolean hasMetadata() { return hasMetadata; }

  /**
   * The entire set of output columns, in output order. Output order is
   * that specified in the SELECT (for an explicit list of columns) or
   * table order (for SELECT * queries).
   * @return the set of output columns in output order
   */

  public List<ScanOutputColumn> outputCols() { return outputCols; }

  public boolean useLegacyWildcardPartition() { return useLegacyStarPlan; }

  public List<String> tableColNames() { return tableColNames; }

  public List<FileMetadataColumnDefn> fileMetadataColDefns() { return fileMetadataColDefns; }

  public String partitionName(int partition) {
    return partitionDesignator + partition;
  }

  public static MajorType partitionColType() {
    return MajorType.newBuilder()
          .setMinorType(MinorType.VARCHAR)
          .setMode(DataMode.OPTIONAL)
          .build();
  }

  public static MajorType nullType() {
    return MajorType.newBuilder()
        .setMinorType(MinorType.NULL)
        .setMode(DataMode.OPTIONAL)
        .build();
  }

  public MajorType columnsArrayType() { return columnsArrayType; }
}
