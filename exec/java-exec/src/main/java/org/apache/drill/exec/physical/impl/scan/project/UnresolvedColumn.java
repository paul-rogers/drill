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

import org.apache.drill.common.expression.SchemaPath;

/**
 * Represents a column in the output rows (record batch, result set) from
 * the scan operator. Each column has an index, which is the column's position
 * within the output tuple.
 */

public class UnresolvedColumn implements ColumnProjection {

  // Defined as ints because the type is extensible.
  // Value 1-10 defined for this base implementation.

  public static final int WILDCARD = 1;
  public static final int UNRESOLVED = 2;

//  public enum ColumnType { TABLE, FILE_METADATA, PARTITION, COLUMNS_ARRAY, WILDCARD, PROJECTED, NULL }
//  public enum ColumnType { WILDCARD, TABLE, SPECIAL }

//  public interface ColumnSemantics {
//
//  }
//
//  public static class RevisedScanColumn extends ScanOutputColumn {
//    private final MajorType type;
//    private final ColumnSemantics semantics;
//
//    protected RevisedScanColumn(SchemaPath inCol, String name, MajorType type, ColumnSemantics semantics) {
//      super(inCol, name);
//      this.type = type;
//      this.semantics = semantics;
//    }
//
//    protected RevisedScanColumn(SchemaPath inCol, String name, ColumnSemantics semantics) {
//      this(inCol, name, null, semantics);
//    }
//
//    @Override
//    public MajorType type() { return type; }
//
//    @SuppressWarnings("unchecked")
//    public <T extends ColumnSemantics> T semantics() { return (T) semantics; }
//
//    @Override
//    public ColumnType columnType() {
//      // TODO Auto-generated method stub
//      return null;
//    }
//
//    @Override
//    protected void visit(int index, Visitor visitor) {
//      // TODO Auto-generated method stub
//
//    }
//  }

//  /**
//   * Represents an unexpanded wildcard. At most one wildcard can appear in
//   * the projection list. Usually, only the wildcard can appear. The wildcard
//   * must be expanded during the projection rewrite process.
//   */
//
//  public static class WildcardColumn extends UnresolvedColumn {
//
//    private WildcardColumn(SchemaPath inCol) {
//      super(inCol, inCol.rootName(), null);
//    }
//
//    public static UnresolvedColumn fromSelect(SchemaPath inCol) {
//      return new WildcardColumn(inCol);
//    }
//
//    @Override
//    public ColumnType columnType() { return ColumnType.WILDCARD; }
//
//    @Override
//    protected void visit(int index, Visitor visitor) {
//      visitor.visitWildcard(index, this);
//    }
//  }

//  public static abstract class TypedColumn extends ScanOutputColumn {
//
//    private final MajorType type;
//
//    protected TypedColumn(SchemaPath inCol, String name, MajorType type) {
//      super(inCol, name);
//      this.type = type;
//    }
//
//    @Override
//    public MajorType type() { return type; }
//  }

//  /**
//   * Represents a desire to use a table column independent of the actual
//   * table schema. May be resolved to either a projected column or null.
//   * <p>
//   * If created from the original SELECT, then will not have a type. If
//   * created based on a prior schema, then the type will be that of the
//   * appearance in the prior schema.
//   */
//
//  public static class RequestedTableColumn extends UnresolvedColumn {
//
//    private RequestedTableColumn(SchemaPath inCol, String name, MajorType type) {
//      super(inCol, name, type);
//    }
//
//    public static RequestedTableColumn fromSelect(SchemaPath inCol) {
//      return new RequestedTableColumn(inCol, inCol.rootName(), ScanLevelProjection.nullType());
//    }
//
//    public static RequestedTableColumn fromUnresolution(UnresolvedColumn resolved) {
//      return new RequestedTableColumn(resolved.source(), resolved.name(), resolved.type());
//    }
//
//    @Override
//    public ColumnType columnType() { return ColumnType.TABLE; }
//
//    @Override
//    protected void visit(int index, Visitor visitor) {
//      visitor.visitTableColumn(index, this);
//    }
//  }

//  /**
//   * Represents Drill's special "columns" column which holds all actual columns
//   * as an array of Varchars. There can be only one such column in the SELECT
//   * list.
//   */
//
//  public static class ColumnsArrayColumn extends UnresolvedColumn {
//
//    private ColumnsArrayColumn(SchemaPath inCol, String name, MajorType type) {
//      super(inCol, name, type);
//    }
//
//    public static ColumnsArrayColumn fromSelect(SchemaPath inCol, MajorType dataType) {
//      return new ColumnsArrayColumn(inCol, inCol.rootName(), dataType);
//    }
//
//    @Override
//    public ColumnType columnType() { return ColumnType.COLUMNS_ARRAY; }
//
//    @Override
//    protected void visit(int index, Visitor visitor) {
//      visitor.visitColumnsArray(index, this);
//    }
//  }

//  public abstract static class ImplicitColumn<T> extends UnresolvedColumn {
//    private final T defn;
//    protected ImplicitColumn(SchemaPath inCol, String name, MajorType type, T defn) {
//      super(inCol, name, type);
//      this.defn = defn;
//    }
//
//    public T definition() { return defn; }
//
//    public boolean isKindOf(Class<? extends Object> defnClass) {
//      return defnClass.isInstance(defn);
//    }
//  }
//
//  public static class MetadataExtension {
//    public final String value;
//
//    public MetadataExtension(String value) {
//      this.value = value;
//    }
//  }

//  public static class SpecialColumn extends UnresolvedColumn {
//
//  }
//  public static class FileMetadataExtension extends MetadataExtension {
//    private final FileMetadataColumnDefn defn;
//
//    public FileMetadataExtension(FileMetadataColumnDefn defn, String value) {
//      super(value);
//      this.defn = defn;
//    }
//  }
//
//  public static class PartitionExtension extends MetadataExtension {
//    private final int partition;
//
//    public PartitionExtension(int partition, String value) {
//      super(value);
//      this.partition = partition;
//    }
//  }
//
//  public abstract static class ConstantColumn extends UnresolvedColumn {
//
//    public final Object value;
//
//    protected ConstantColumn(SchemaPath inCol, String name, MajorType type, Object value) {
//      super(inCol, name, type);
//      this.value = value;
//    }
//
//    @Override
//    protected void buildString(StringBuilder buf) {
//      buf.append(", value=");
//      if (value == null) {
//        buf.append("null");
//      } else if (value instanceof String) {
//        buf.append("\"");
//        buf.append(value);
//        buf.append("\"");
//      } else {
//        buf.append(value.toString());
//      }
//    }
//
//    @SuppressWarnings("unchecked")
//    public <T extends Object> T value() { return (T) value; }
//  }
//
//  /**
//   * Base class for the various static (implicit) columns. Holds the
//   * value of the column.
//   */
//
//  public abstract static class MetadataColumn extends UnresolvedColumn {
//
//    public final String value;
//
//    protected MetadataColumn(SchemaPath inCol, String name, MajorType type, String value) {
//      super(inCol, name, type);
//      this.value = value;
//    }
//
//    @Override
//    protected void buildString(StringBuilder buf) {
//      buf.append(", value=")
//         .append(value == null ? "null" :
//                 "\"" + value + "\"");
//    }
//
//    public String value() { return value; }
//  }

//  /**
//   * Represents an output column created from an implicit column. Since
//   * values are known before reading data, the value is provided
//   * along with the column definition.
//   */
//
//  public static class FileMetadataColumn extends MetadataColumn {
//
//    private final FileMetadataColumnDefn defn;
//
//    private FileMetadataColumn(SchemaPath inCol, String name, FileMetadataColumnDefn defn, String value) {
//      super(inCol, name, defn.dataType(), value);
//      this.defn = defn;
//    }
//
//    public static FileMetadataColumn fromSelect(SchemaPath inCol, FileMetadataColumnDefn defn) {
//      return new FileMetadataColumn(inCol, inCol.rootName(), defn, null);
//    }
//
//    public static MetadataColumn fromWildcard(SchemaPath inCol,
//        FileMetadataColumnDefn defn, FileMetadata fileInfo) {
//      return new FileMetadataColumn(inCol, defn.colName(), defn,
//          valueOf(defn, fileInfo));
//    }
//
//    public static FileMetadataColumn resolved(SchemaPath inCol,
//        FileMetadataColumnDefn defn, FileMetadata fileInfo) {
//      return new FileMetadataColumn(inCol, inCol.rootName(), defn,
//          valueOf(defn, fileInfo));
//    }
//
//    private static String valueOf(FileMetadataColumnDefn defn, FileMetadata fileInfo) {
//      return defn.defn.getValue(fileInfo.filePath());
//    }
//
//    @Override
//    public ColumnType columnType() { return ColumnType.FILE_METADATA; }
//
//    @Override
//    public String name() {
//      if (inCol.isWildcard()) {
//        return defn.colName;
//      } else {
//        return super.name();
//      }
//    }
//
//    @Override
//    protected void buildString(StringBuilder buf) {
//      super.buildString(buf);
//      buf.append(", defn=")
//         .append(defn);
//    }
//
//    @Override
//    protected void visit(int index, Visitor visitor) {
//      visitor.visitFileInfoColumn(index, this);
//    }
//
//    public FileMetadataColumn cloneWithValue(FileMetadata fileInfo) {
//      return new FileMetadataColumn(inCol, name(), defn, valueOf(defn, fileInfo));
//    }
//
//    @Override
//    public UnresolvedColumn unresolve() {
//      return new FileMetadataColumn(inCol, name(), defn, null);
//    }
//  }

//  /**
//   * Partition output column for "dir<n>" for some n.
//   * Data type is optional because some files may be more deeply
//   * nested than others, so some files may have, say a dir2
//   * while others do not.
//   * <p>
//   * The "dir" portion is customizable via a session option.
//   * <p>
//   * The value of the partition is known up front, and so the value
//   * is stored in this column definition.
//   */
//
//  public static class PartitionColumn extends MetadataColumn {
//    private final int partition;
//
//    private PartitionColumn(SchemaPath inCol, String name, MajorType dataType, int partition, String value) {
//      super(inCol, name, dataType, value);
//      this.partition = partition;
//    }
//
//    public static PartitionColumn fromSelect(SchemaPath inCol, int partition) {
//      return new PartitionColumn(inCol, inCol.rootName(), dataType(), partition, null);
//    }
//
//    public static MetadataColumn fromWildcard(SchemaPath inCol, String name,
//        int partition, FileMetadata fileInfo) {
//      return new PartitionColumn(inCol, name, dataType(), partition,
//          fileInfo.partition(partition));
//    }
//
//    public static PartitionColumn resolved(SchemaPath inCol, int partition,
//        FileMetadata fileInfo) {
//      return new PartitionColumn(inCol, inCol.rootName(), dataType(), partition,
//          fileInfo.partition(partition));
//    }
//
//    private static MajorType dataType() {
//      return FileMetadataProjection.partitionColType();
//    }
//
//    @Override
//    public ColumnType columnType() { return ColumnType.PARTITION; }
//
//    @Override
//    protected void buildString(StringBuilder buf) {
//      super.buildString(buf);
//      buf.append(", partition=")
//         .append(partition);
//    }
//
//    public int partition() { return partition; }
//
//    @Override
//    protected void visit(int index, Visitor visitor) {
//      visitor.visitPartitionColumn(index, this);
//    }
//
//    public PartitionColumn cloneWithValue(FileMetadata fileInfo) {
//      return new PartitionColumn(inCol, name(), type(),
//          partition, fileInfo.partition(partition));
//    }
//
//    @Override
//    public UnresolvedColumn unresolve() {
//      return new PartitionColumn(inCol, name(), type(), partition, null);
//    }
//  }



//  /**
//   * Visit each output column via a typed method to allow clean processing
//   * of each column type without casts. Classic Gang-of-Four pattern.
//   */
//
//  public static class Visitor {
//
//    public void visit(ScanLevelProjection plan) {
//      visit(plan.outputCols());
//    }
//
//    public void visit(List<UnresolvedColumn> cols) {
//      for (int i = 0; i < cols.size(); i++) {
//        cols.get(i).visit(i, this);
//      }
//    }
//
//    protected void visitPartitionColumn(int index, PartitionColumn col) {
//      visitColumn(index, col);
//    }
//
//    protected void visitFileInfoColumn(int index, FileMetadataColumn col) {
//      visitColumn(index, col);
//    }
//
//    protected void visitColumnsArray(int index, ColumnsArrayColumn col) {
//      visitColumn(index, col);
//    }
//
//    protected void visitTableColumn(int index, RequestedTableColumn col) {
//      visitColumn(index, col);
//    }
//
//    protected void visitWildcard(int index, WildcardColumn col) {
//      visitColumn(index, col);
//    }
//
//    public void visitProjection(int index, ProjectedColumn col) {
//      visitColumn(index, col);
//    }
//
//    public void visitNullColumn(int index, NullColumn col) {
//      visitColumn(index, col);
//    }
//
//    protected void visitColumn(int index, UnresolvedColumn col) { }
//  }

//  public static class NullColumnProjection extends UnresolvedColumn {
//
//    public NullColumnProjection(SchemaPath inCol) {
//      super(inCol);
//    }
//
//    @Override
//    public ColumnType columnType() { return null; }
//
//    @Override
//    protected void visit(int index, Visitor visitor) { }
//  }

  /**
   * The original physical plan column to which this output column
   * maps. In some cases, multiple output columns map map the to the
   * same "input" (to the projection process) column.
   */

  protected final SchemaPath inCol;

  protected final int type;


  public UnresolvedColumn(SchemaPath inCol, int type) {
    this.inCol = inCol;
    this.type = type;
  }

//  public void setExtension(Object extn) {
//    this.extension = extn;
//  }

//  @SuppressWarnings("unchecked")
//  public <T> T extension() { return (T) extension; }
//
//  public boolean kindOf(Class<?> type) {
//    return extension == null ? false : type.isInstance(extension);
//  }

  @Override
  public int nodeType() { return type; }
  @Override
  public String name() { return inCol.rootName(); }
  public SchemaPath source() { return inCol; }
//  public MajorType type() { return type; }
  public UnresolvedColumn unresolve() { return this; }

  protected void buildString(StringBuilder buf) { }
//  protected abstract void visit(int index, UnresolvedColumn.Visitor visitor);

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder()
        .append("[")
        .append(getClass().getSimpleName())
        .append(" name=\"")
        .append(name())
        .append(", colType=")
        .append(nodeType())
        .append(", type=");
//    MajorType type = type();
//    if (type == null) {
//      buf.append("null");
//    } else {
//      buf.append("[")
//         .append(type.toString().trim().replaceAll("\n", ", "))
//         .append("]");
//    }
    buildString(buf);
    buf.append(", id=")
      .append(nodeType())
      .append("]");
    return buf.toString();
  }

  @Override
  public boolean resolved() { return false; }

//  @VisibleForTesting
//  public MaterializedField schema() {
//    return MaterializedField.create(name(), type());
//  }

//  @VisibleForTesting
//  public static TupleMetadata schema(List<UnresolvedColumn> output) {
//    TupleMetadata schema = new TupleSchema();
//    for (UnresolvedColumn col : output ) {
//      schema.add(col.schema());
//    }
//    return schema;
//  }
}