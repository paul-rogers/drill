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

import java.util.List;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.physical.impl.scan.project.FileMetadataColumnsParser.FileMetadata;
import org.apache.drill.exec.physical.impl.scan.project.FileMetadataColumnsParser.FileMetadataColumnDefn;
import org.apache.drill.exec.physical.impl.scan.project.FileMetadataColumnsParser.FileMetadataProjection;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleSchema;

import com.google.common.annotations.VisibleForTesting;

/**
 * Represents a column in the output rows (record batch, result set) from
 * the scan operator. Each column has an index, which is the column's position
 * within the output tuple.
 */

public abstract class ScanOutputColumn {
  public enum ColumnType { TABLE, FILE_METADATA, PARTITION, COLUMNS_ARRAY, WILDCARD, PROJECTED, NULL }

  public static class WildcardColumn extends ScanOutputColumn {

    private WildcardColumn(RequestedColumn inCol) {
      super(inCol, inCol.name());
    }

    public static ScanOutputColumn fromSelect(RequestedColumn inCol) {
      return new WildcardColumn(inCol);
    }

    @Override
    public ColumnType columnType() { return ColumnType.WILDCARD; }

    @Override
    protected void visit(int index, Visitor visitor) {
      visitor.visitWildcard(index, this);
    }
  }

  public static abstract class TypedColumn extends ScanOutputColumn {

    private final MajorType type;

    protected TypedColumn(RequestedColumn inCol, String name, MajorType type) {
      super(inCol, name);
      this.type = type;
    }

    @Override
    public MajorType type() { return type; }
  }

  /**
   * Represents a desire to use a table column independent of the actual
   * table schema. May be resolved to either a projected column or null.
   * <p>
   * If created from the original SELECT, then will not have a type. If
   * created based on a prior schema, then the type will be that of the
   * appearance in the prior schema.
   */

  public static class RequestedTableColumn extends TypedColumn {

    private RequestedTableColumn(RequestedColumn inCol, String name, MajorType type) {
      super(inCol, name, type);
    }

    public static RequestedTableColumn fromSelect(RequestedColumn inCol) {
      return new RequestedTableColumn(inCol, inCol.name(), ScanLevelProjection.nullType());
    }

    public static RequestedTableColumn fromUnresolution(TypedColumn resolved) {
      return new RequestedTableColumn(resolved.source(), resolved.name(), resolved.type());
    }

    @Override
    public ColumnType columnType() { return ColumnType.TABLE; }

    @Override
    protected void visit(int index, Visitor visitor) {
      visitor.visitTableColumn(index, this);
    }
  }

  /**
   * Represents Drill's special "columns" column which holds all actual columns
   * as an array of Varchars. There can be only one such column in the SELECT
   * list.
   */

  public static class ColumnsArrayColumn extends TypedColumn {

    private ColumnsArrayColumn(RequestedColumn inCol, String name, MajorType type) {
      super(inCol, name, type);
    }

    public static ColumnsArrayColumn fromSelect(RequestedColumn inCol, MajorType dataType) {
      return new ColumnsArrayColumn(inCol, inCol.name(), dataType);
    }

    @Override
    public ColumnType columnType() { return ColumnType.COLUMNS_ARRAY; }

    @Override
    protected void visit(int index, Visitor visitor) {
      visitor.visitColumnsArray(index, this);
    }
  }

  /**
   * Base class for the various static (implicit) columns. Holds the
   * value of the column.
   */

  public abstract static class MetadataColumn extends TypedColumn {

    public final String value;

    protected MetadataColumn(RequestedColumn inCol, String name, MajorType type, String value) {
      super(inCol, name, type);
      this.value = value;
    }

    @Override
    protected void buildString(StringBuilder buf) {
      buf.append(", value=")
         .append(value == null ? "null" :
                 "\"" + value + "\"");
    }

    public String value() { return value; }
  }

  /**
   * Represents an output column created from an implicit column. Since
   * values are known before reading data, the value is provided
   * along with the column definition.
   */

  public static class FileMetadataColumn extends MetadataColumn {

    private final FileMetadataColumnDefn defn;

    private FileMetadataColumn(RequestedColumn inCol, String name, FileMetadataColumnDefn defn, String value) {
      super(inCol, name, defn.dataType(), value);
      this.defn = defn;
    }

    public static FileMetadataColumn fromSelect(RequestedColumn inCol, FileMetadataColumnDefn defn) {
      return new FileMetadataColumn(inCol, inCol.name(), defn, null);
    }

    public static MetadataColumn fromWildcard(RequestedColumn inCol,
        FileMetadataColumnDefn defn, FileMetadata fileInfo) {
      return new FileMetadataColumn(inCol, defn.colName(), defn,
          valueOf(defn, fileInfo));
    }

    public static FileMetadataColumn resolved(RequestedColumn inCol,
        FileMetadataColumnDefn defn, FileMetadata fileInfo) {
      return new FileMetadataColumn(inCol, inCol.name(), defn,
          valueOf(defn, fileInfo));
    }

    private static String valueOf(FileMetadataColumnDefn defn, FileMetadata fileInfo) {
      return defn.defn.getValue(fileInfo.filePath());
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
    protected void buildString(StringBuilder buf) {
      super.buildString(buf);
      buf.append(", defn=")
         .append(defn);
    }

    @Override
    protected void visit(int index, Visitor visitor) {
      visitor.visitFileInfoColumn(index, this);
    }

    public FileMetadataColumn cloneWithValue(FileMetadata fileInfo) {
      return new FileMetadataColumn(inCol, name(), defn, valueOf(defn, fileInfo));
    }

    @Override
    public ScanOutputColumn unresolve() {
      return new FileMetadataColumn(inCol, name(), defn, null);
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

    private PartitionColumn(RequestedColumn inCol, String name, MajorType dataType, int partition, String value) {
      super(inCol, name, dataType, value);
      this.partition = partition;
    }

    public static PartitionColumn fromSelect(RequestedColumn inCol, int partition) {
      return new PartitionColumn(inCol, inCol.name(), dataType(), partition, null);
    }

    public static MetadataColumn fromWildcard(RequestedColumn inCol, String name,
        int partition, FileMetadata fileInfo) {
      return new PartitionColumn(inCol, name, dataType(), partition,
          fileInfo.partition(partition));
    }

    public static PartitionColumn resolved(RequestedColumn inCol, int partition,
        FileMetadata fileInfo) {
      return new PartitionColumn(inCol, inCol.name(), dataType(), partition,
          fileInfo.partition(partition));
    }

    private static MajorType dataType() {
      return FileMetadataProjection.partitionColType();
    }

    @Override
    public ColumnType columnType() { return ColumnType.PARTITION; }

    @Override
    protected void buildString(StringBuilder buf) {
      super.buildString(buf);
      buf.append(", partition=")
         .append(partition);
    }

    public int partition() { return partition; }

    @Override
    protected void visit(int index, Visitor visitor) {
      visitor.visitPartitionColumn(index, this);
    }

    public PartitionColumn cloneWithValue(FileMetadata fileInfo) {
      return new PartitionColumn(inCol, name(), type(),
          partition, fileInfo.partition(partition));
    }

    @Override
    public ScanOutputColumn unresolve() {
      return new PartitionColumn(inCol, name(), type(), partition, null);
    }
  }

  /**
   * Represents a selected column which does not match a
   * table column, and so has a null value.
   */

  public static class NullColumn extends TypedColumn {

    private NullColumn(RequestedColumn inCol, String name, MajorType type) {
      super(inCol, name, type);
    }

    public static NullColumn fromResolution(RequestedTableColumn tableCol) {
      return new NullColumn(tableCol.source(), tableCol.name(), tableCol.type());
    }

    @Override
    public ColumnType columnType() {
      return ColumnType.NULL;
    }

    @Override
    protected void visit(int index, Visitor visitor) {
      visitor.visitNullColumn(index, this);
    }

    @Override
    public RequestedTableColumn unresolve() {
      return RequestedTableColumn.fromUnresolution(this);
    }
  }

  /**
   * Represents a table column projected to the output.
   */

  public static class ProjectedColumn extends TypedColumn {

    private final int columnIndex;
    private MaterializedField schema;

    private ProjectedColumn(RequestedColumn inCol, String name,
        int columnIndex, MaterializedField schema) {
      super(inCol, name, schema.getType());
      this.schema = schema;
      this.columnIndex = columnIndex;
    }

    public static ProjectedColumn fromWildcard(WildcardColumn col,
        int columnIndex, MaterializedField column) {
      return new ProjectedColumn(col.source(), column.getName(),
          columnIndex, column);
    }

    public static ProjectedColumn fromResolution(RequestedTableColumn col,
        int columnIndex, MaterializedField column) {
      return new ProjectedColumn(col.source(), col.name(),
          columnIndex, column);
    }

    public static ProjectedColumn fromColumnsArray(ColumnsArrayColumn col) {
      return new ProjectedColumn(col.source(), col.inCol.name(), 0,
          MaterializedField.create(col.inCol.name(), col.type()));
    }

    @Override
    public ColumnType columnType() {
      return ColumnType.PROJECTED;
    }

    public int columnIndex() { return columnIndex; }

    @Override
    public MaterializedField schema() { return schema; }

    @Override
    protected void buildString(StringBuilder buf) {
      buf.append(", index=")
         .append(columnIndex);
    }

    @Override
    protected void visit(int index, Visitor visitor) {
      visitor.visitProjection(index, this);
    }

    @Override
    public RequestedTableColumn unresolve() {
      return RequestedTableColumn.fromUnresolution(this);
    }
  }

  /**
   * Visit each output column via a typed method to allow clean processing
   * of each column type without casts. Classic Gang-of-Four pattern.
   */

  public static class Visitor {

    public void visit(ScanLevelProjection plan) {
      visit(plan.outputCols());
    }

    public void visit(List<ScanOutputColumn> cols) {
      for (int i = 0; i < cols.size(); i++) {
        cols.get(i).visit(i, this);
      }
    }

    protected void visitPartitionColumn(int index, PartitionColumn col) {
      visitColumn(index, col);
    }

    protected void visitFileInfoColumn(int index, FileMetadataColumn col) {
      visitColumn(index, col);
    }

    protected void visitColumnsArray(int index, ColumnsArrayColumn col) {
      visitColumn(index, col);
    }

    protected void visitTableColumn(int index, RequestedTableColumn col) {
      visitColumn(index, col);
    }

    protected void visitWildcard(int index, WildcardColumn col) {
      visitColumn(index, col);
    }

    public void visitProjection(int index, ProjectedColumn col) {
      visitColumn(index, col);
    }

    public void visitNullColumn(int index, NullColumn col) {
      visitColumn(index, col);
    }

    protected void visitColumn(int index, ScanOutputColumn col) { }
  }

  public static class NullColumnProjection extends ScanOutputColumn {

    public NullColumnProjection(RequestedColumn inCol) {
      super(inCol);
    }

    @Override
    public ColumnType columnType() { return null; }

    @Override
    protected void visit(int index, Visitor visitor) { }
  }

  protected final RequestedColumn inCol;
  protected final String name;

  public ScanOutputColumn(RequestedColumn inCol) {
    this(inCol, null);
  }

  public ScanOutputColumn(RequestedColumn inCol, String name) {
    this.inCol = inCol;
    this.name = name;
  }

  public abstract ScanOutputColumn.ColumnType columnType();
  public String name() { return name; }
  public RequestedColumn source() { return inCol; }
  public MajorType type() { return ScanLevelProjection.nullType(); }
  public ScanOutputColumn unresolve() { return this; }

  protected void buildString(StringBuilder buf) { }
  protected abstract void visit(int index, ScanOutputColumn.Visitor visitor);


  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder()
        .append("[")
        .append(getClass().getSimpleName())
        .append(" name=\"")
        .append(name())
        .append(", colType=")
        .append(columnType())
        .append(", type=");
    MajorType type = type();
    if (type == null) {
      buf.append("null");
    } else {
      buf.append("[")
         .append(type.toString().trim().replaceAll("\n", ", "))
         .append("]");
    }
    buildString(buf);
    buf.append("]");
    return buf.toString();
  }

  @VisibleForTesting
  public MaterializedField schema() {
    return MaterializedField.create(name(), type());
  }

  @VisibleForTesting
  public static TupleMetadata schema(List<ScanOutputColumn> output) {
    TupleMetadata schema = new TupleSchema();
    for (ScanOutputColumn col : output ) {
      schema.add(col.schema());
    }
    return schema;
  }
}