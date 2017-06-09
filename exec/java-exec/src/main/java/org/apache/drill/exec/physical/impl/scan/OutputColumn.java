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

import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.QuerySelectionPlan.SelectColumn;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.hadoop.fs.Path;

/**
 * Represents a column in the output rows (record batch, result set) from
 * the scan operator. Each column has an index, which is the column's position
 * within the output tuple.
 */

public abstract class OutputColumn {
  public enum ColumnType { TABLE, FILE_METADATA, PARTITION, COLUMNS_ARRAY, WILDCARD, PROJECTED, NULL }

  public static class WildcardColumn extends OutputColumn {

    public final boolean includePartitions;

    public WildcardColumn(QuerySelectionPlan.SelectColumn inCol, boolean includePartitions) {
      super(inCol);
      this.includePartitions = includePartitions;
    }

    @Override
    public ColumnType columnType() { return ColumnType.WILDCARD; }

    @Override
    protected void buildString(StringBuilder buf) {
      super.buildString(buf);
      buf.append(", partitions=")
         .append(includePartitions);
    }

    public boolean includePartitions() { return includePartitions; }

    @Override
    protected void visit(OutputColumn.Visitor visitor) {
      visitor.visitWildcard(this);
    }
  }

  public static class ExpectedTableColumn extends OutputColumn {

    public ExpectedTableColumn(QuerySelectionPlan.SelectColumn inCol) {
      super(inCol);
    }

    @Override
    public ColumnType columnType() { return ColumnType.TABLE; }

    @Override
    protected void visit(OutputColumn.Visitor visitor) {
      visitor.visitTableColumn(this);
    }
  }

  public static abstract class TypedColumn extends OutputColumn {

    private final MajorType type;

    public TypedColumn(QuerySelectionPlan.SelectColumn inCol, MajorType type) {
      super(inCol);
      this.type = type;
    }

    @Override
    public MajorType type() { return type; }

    @Override
    protected void buildString(StringBuilder buf) {
      super.buildString(buf);
      buf.append(", type=")
         .append(type.toString());
    }

    public MaterializedField schema() {
      return MaterializedField.create(name(), type);
    }
  }

  /**
   * Represents Drill's special "columns" column which holds all actual columns
   * as an array of Varchars. There can be only one such column in the SELECT
   * list.
   */

  public static class ColumnsArrayColumn extends TypedColumn {

    public ColumnsArrayColumn(QuerySelectionPlan.SelectColumn inCol) {
      super(inCol,
          MajorType.newBuilder()
          .setMinorType(MinorType.VARCHAR)
          .setMode(DataMode.REPEATED)
          .build());
    }

    @Override
    public ColumnType columnType() { return ColumnType.COLUMNS_ARRAY; }

    @Override
    protected void visit(OutputColumn.Visitor visitor) {
      visitor.visitColumnsArray(this);
    }
  }

  /**
   * Base class for the various static (implicit) columns. Holds the
   * value of the column.
   */

  public abstract static class MetadataColumn extends TypedColumn {

    public final String value;

    public MetadataColumn(QuerySelectionPlan.SelectColumn inCol, MajorType type) {
      this(inCol, type, null);
    }

    public MetadataColumn(QuerySelectionPlan.SelectColumn inCol, MajorType type, String value) {
      super(inCol, type);
      this.value = value;
    }

    public abstract String value(OutputColumn.ResolvedFileInfo fileInfo);
    public String value() { return value; }
  }

  /**
   * Represents an output column created from an implicit column. Since
   * values are known before reading data, the value is provided
   * along with the column definition.
   */

  public static class FileMetadataColumn extends MetadataColumn {

    private final QuerySelectionPlan.FileMetadataColumnDefn defn;

    public FileMetadataColumn(QuerySelectionPlan.SelectColumn inCol, QuerySelectionPlan.FileMetadataColumnDefn defn) {
      this(inCol, defn, null);
    }
    public FileMetadataColumn(QuerySelectionPlan.SelectColumn inCol, QuerySelectionPlan.FileMetadataColumnDefn defn, String value) {
      super(inCol, MajorType.newBuilder()
            .setMinorType(MinorType.VARCHAR)
            .setMode(DataMode.REQUIRED)
            .build(), value);
      this.defn = defn;
    }

    @Override
    public ColumnType columnType() { return ColumnType.FILE_METADATA; }

    @Override
    public String name() {
      if (inCol.isWildcard()) {
        return defn.colName;
      } else {
        return super.name();
      }
    }

    @Override
    public String value(OutputColumn.ResolvedFileInfo fileInfo) {
      return defn.defn.getValue(fileInfo.filePath());
    }

    @Override
    protected void buildString(StringBuilder buf) {
      super.buildString(buf);
      buf.append(", defn=")
         .append(defn);
    }

    @Override
    protected void visit(OutputColumn.Visitor visitor) {
      visitor.visitFileInfoColumn(this);
    }

    public OutputColumn cloneWithValue(OutputColumn.ResolvedFileInfo fileInfo) {
      return new FileMetadataColumn(inCol, defn, value(fileInfo));
    }
  }

  /**
   * Partition output column for "dir<n>" for some n.
   * Data type is optional because some files may be more deeply
   * nested than others, so some files may have, say a dir2
   * while others do not.
   * <p>
   * The "dir" portion is customizable via a session option.
   * <p>
   * The value of the partition is known up front, and so the value
   * is stored in this column definition.
   */

  public static class PartitionColumn extends MetadataColumn {
    private final int partition;

    public PartitionColumn(QuerySelectionPlan.SelectColumn inCol, int partition) {
      this(inCol, partition, null);
    }

    public PartitionColumn(QuerySelectionPlan.SelectColumn inCol, int partition, String value) {
      super(inCol, MajorType.newBuilder()
          .setMinorType(MinorType.VARCHAR)
          .setMode(DataMode.OPTIONAL)
          .build(), value);
      this.partition = partition;
    }

    @Override
    public ColumnType columnType() { return ColumnType.PARTITION; }

    @Override
    public String value(OutputColumn.ResolvedFileInfo fileInfo) {
      return fileInfo.partition(partition);
    }

    @Override
    protected void buildString(StringBuilder buf) {
      super.buildString(buf);
      buf.append(", partition=")
         .append(partition);
    }

    public int partition() { return partition; }

    @Override
    protected void visit(OutputColumn.Visitor visitor) {
      visitor.visitPartitionColumn(this);
    }

    public OutputColumn cloneWithValue(OutputColumn.ResolvedFileInfo fileInfo) {
      return new PartitionColumn(inCol, partition, value(fileInfo));
    }
  }

  /**
   * Given a pair of file and directory parameters, expand
   * the selection list to compute the static values.
   */

  public static class NullColumn extends OutputColumn {

    public NullColumn(SelectColumn inCol) {
      super(inCol);
    }

    @Override
    public ColumnType columnType() {
      return ColumnType.NULL;
    }

    @Override
    protected void visit(
        OutputColumn.Visitor visitor) {
      visitor.visitNullColumn(this);
    }
  }

  /**
   * Given a static projection plan, expand the schema given
   * a actual table or batch schema.
   */

  public static class ProjectedColumn extends OutputColumn {

    private MaterializedField schema;
    private int tableColumnIndex;

    public ProjectedColumn(SelectColumn inCol, int index,
        MaterializedField schema, String name) {
      super(inCol, name);
      this.schema = MaterializedField.create(inCol.name(), schema.getType());
      this.tableColumnIndex = index;
    }

    @Override
    public ColumnType columnType() {
      return ColumnType.PROJECTED;
    }

    @Override
    public MajorType type() { return schema.getType(); }

    @Override
    protected void visit(
        OutputColumn.Visitor visitor) {
      visitor.visitProjection(this);
    }

  }

  /**
   * Specify the file name and optional selection root. If the selection root
   * is provided, then partitions are defined as the portion of the file name
   * that is not also part of the selection root. That is, if selection root is
   * /a/b and the file path is /a/b/c/d.csv, then dir0 is c.
   */

  public static class ResolvedFileInfo {

    private final Path filePath;
    private final String[] dirPath;

    public ResolvedFileInfo(Path filePath, String selectionRoot) {
      this.filePath = filePath;
      if (selectionRoot == null) {
        dirPath = null;
        return;
      }

      // Result of splitting /x/y is ["", "x", "y"], so ignore first.

      String[] r = Path.getPathWithoutSchemeAndAuthority(new Path(selectionRoot)).toString().split("/");

      // Result of splitting "/x/y/z.csv" is ["", "x", "y", "z.csv"], so ignore first and last

      String[] p = Path.getPathWithoutSchemeAndAuthority(filePath).toString().split("/");

      if (p.length - 1 < r.length) {
        throw new IllegalArgumentException("Selection root of \"" + selectionRoot +
                                        "\" is shorter than file path of \"" + filePath.toString() + "\"");
      }
      for (int i = 1; i < r.length; i++) {
        if (! r[i].equals(p[i])) {
          throw new IllegalArgumentException("Selection root of \"" + selectionRoot +
              "\" is not a leading path of \"" + filePath.toString() + "\"");
        }
      }
      dirPath = ArrayUtils.subarray(p, r.length, p.length - 1);
    }

    public ResolvedFileInfo(Path fileName, Path rootDir) {
      this(fileName, rootDir == null ? null : rootDir.toString());
    }

    public Path filePath() { return filePath; }

    public String partition(int index) {
      if (dirPath == null ||  dirPath.length <= index) {
        return null;
      }
      return dirPath[index];
    }

    public int dirPathLength() {
      return dirPath == null ? 0 : dirPath.length;
    }
  }

  public static class Visitor {

    public void visit(QuerySelectionPlan plan) {
      visit(plan.outputCols());
    }

    public void visit(List<OutputColumn> cols) {
      for (OutputColumn col : cols) {
        col.visit(this);
      }
    }

    protected void visitPartitionColumn(PartitionColumn col) {
      visitColumn(col);
    }

    protected void visitFileInfoColumn(FileMetadataColumn col) {
      visitColumn(col);
    }

    protected void visitColumnsArray(ColumnsArrayColumn col) {
      visitColumn(col);
    }

    protected void visitTableColumn(ExpectedTableColumn col) {
      visitColumn(col);
    }

    protected void visitWildcard(WildcardColumn col) {
      visitColumn(col);
    }

    protected void visitColumn(OutputColumn col) { }

    public void visitProjection(ProjectedColumn col) {
      visitColumn(col);
    }

    public void visitNullColumn(NullColumn col) {
      visitColumn(col);
    }
  }

  protected final SelectColumn inCol;
  protected final String name;

  public OutputColumn(SelectColumn inCol) {
    this(inCol, inCol.name());
  }

  public OutputColumn(SelectColumn inCol, String name) {
    this.inCol = inCol;
    this.name = name;
  }

  public abstract OutputColumn.ColumnType columnType();

  public String name() { return name; }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder()
        .append("[")
        .append(getClass().getSimpleName());
    buildString(buf);
    buf.append("]");
    return buf.toString();
  }

  protected void buildString(StringBuilder buf) { }

  public SelectColumn source() { return inCol; }
  public MajorType type() { return null; }

  protected abstract void visit(OutputColumn.Visitor visitor);
}