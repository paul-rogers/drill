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
package org.apache.drill.exec.record;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;

/**
 * Defines the schema of a tuple: either the top-level row or a nested
 * "map" (really structure). A schema is a collection of columns (backed
 * by vectors in the loader itself.) Columns are accessible by name or
 * index. New columns may be added at any time; the new column takes the
 * next available index.
 */

public class TupleSchema implements TupleMetadata {

  private static class TupleStructure {
    protected final TupleSchema parent;
    protected final int index;

    protected TupleStructure(TupleSchema parent, int index) {
      this.parent = parent;
      this.index = index;
    }
  }

  public static abstract class AbstractColumnMetadata implements ColumnMetadata {

    private TupleStructure anchor;

    public void bind(TupleStructure anchor) {
      this.anchor = anchor;
    }

    @Override
    public int index() { return anchor.index; }
    @Override
    public TupleMetadata parent() { return anchor.parent; }

    @Override
    public String name() { return schema().getName(); }
    @Override
    public MajorType majorType() { return schema().getType(); }
    @Override
    public MinorType type() { return schema().getType().getMinorType(); }
    @Override
    public DataMode mode() { return schema().getDataMode(); }

    @Override
    public String fullName( ) {
      ColumnMetadata parentMap = parent().parent();
      if (parentMap == null) {
        return name();
      } else {
        return parentMap.fullName() + "." + name();
      }
    }

    @Override
    public boolean isEquivalent(ColumnMetadata other) {
      return schema().isEquivalent(other.schema());
    }

    @Override
    public String toString() {
      return new StringBuilder()
          .append("[")
          .append(getClass().getSimpleName())
          .append(" ")
          .append(schema().toString())
          .append("]")
          .toString();
    }
  }

  public static abstract class BaseColumnMetadata extends AbstractColumnMetadata {
    protected final MaterializedField schema;

    public BaseColumnMetadata(MaterializedField schema) {
      this.schema = schema;
    }

    @Override
    public MaterializedField schema() { return schema; }
  }

  public static class PrimitiveColumnMetadata extends BaseColumnMetadata {

    private int allocationSize;

    public PrimitiveColumnMetadata(MaterializedField schema) {
      super(schema);
    }

    @Override
    public StructureType structureType() { return StructureType.PRIMITIVE; }
    @Override
    public TupleMetadata mapSchema() { return null; }

    @Override
    public int allocationSize() {
      return allocationSize == 0
          ? TypeHelper.getSize(schema.getType())
          : allocationSize;
    }
  }

  public static class MapColumnMetadata  extends BaseColumnMetadata {
    private final TupleMetadata mapSchema;

    public MapColumnMetadata(MaterializedField schema) {
      super(schema);
      mapSchema = new TupleSchema(this);
      for (MaterializedField child : schema.getChildren()) {
        mapSchema.add(child);
      }
    }

    @Override
    public StructureType structureType() { return StructureType.TUPLE; }
    @Override
    public TupleMetadata mapSchema() { return mapSchema; }

    @Override
    public int allocationSize() { return 0; }
  }

  private final MapColumnMetadata parentMap;
  private final TupleNameSpace<ColumnMetadata> nameSpace = new TupleNameSpace<>();

  public TupleSchema() { this((MapColumnMetadata) null); }

  public TupleSchema(MapColumnMetadata parentMap) {
    this.parentMap = parentMap;
  }

  public static TupleMetadata fromFields(MapColumnMetadata parent, Iterable<MaterializedField> fields) {
    TupleMetadata tuple = new TupleSchema(parent);
    for (MaterializedField field : fields) {
      tuple.add(field);
    }
    return tuple;
  }

  public static TupleMetadata fromFields(Iterable<MaterializedField> fields) {
    return fromFields(null, fields);
  }

  public TupleMetadata copy() {
    TupleMetadata tuple = new TupleSchema();
    for (ColumnMetadata md : this) {
      tuple.add(md.schema());
    }
    return tuple;
  }

  @Override
  public ColumnMetadata add(MaterializedField field) {
    AbstractColumnMetadata md;
    if (field.getType().getMinorType() == MinorType.MAP) {
      md = new MapColumnMetadata(field);
    } else {
      md = new PrimitiveColumnMetadata(field);
    }
    addColumn(md);
    return md;
  }

  /**
   * Add a column metadata column created by the caller. Used for specialized
   * cases beyond those handled by {@link #add(MaterializedField)}.
   *
   * @param md the custom column metadata which must have the correct
   * index set (from {@link #size()}
   */

  public void addColumn(AbstractColumnMetadata md) {
    md.bind(new TupleStructure(this, nameSpace.count()));
    nameSpace.add(md.name(), md);
  }

  @Override
  public MaterializedField column(String name) {
    ColumnMetadata md = metadata(name);
    return md == null ? null : md.schema();
  }

  @Override
  public ColumnMetadata metadata(String name) {
    return nameSpace.get(name);
  }

  @Override
  public int index(String name) {
    return nameSpace.indexOf(name);
  }

  @Override
  public MaterializedField column(int index) {
    ColumnMetadata md = metadata(index);
    return md == null ? null : md.schema();
  }

  @Override
  public ColumnMetadata metadata(int index) {
    return nameSpace.get(index);
  }

  @Override
  public MapColumnMetadata parent() { return parentMap; }

  @Override
  public int size() { return nameSpace.count(); }

  @Override
  public boolean isEmpty() { return nameSpace.count( ) == 0; }

  @Override
  public Iterator<ColumnMetadata> iterator() {
    return nameSpace.iterator();
  }

  @Override
  public boolean isEquivalent(TupleMetadata other) {
    TupleSchema otherSchema = (TupleSchema) other;
    if (nameSpace.count() != otherSchema.nameSpace.count()) {
      return false;
    }
    for (int i = 0; i < nameSpace.count(); i++) {
      if (! nameSpace.get(i).isEquivalent(otherSchema.nameSpace.get(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public List<MaterializedField> toFieldList() {
    List<MaterializedField> cols = new ArrayList<>();
    for (ColumnMetadata md : nameSpace) {
      cols.add(md.schema());
    }
    return cols;
  }

  public BatchSchema toBatchSchema(SelectionVectorMode svMode) {
    return new BatchSchema(svMode, toFieldList());
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder()
        .append("[")
        .append(getClass().getSimpleName())
        .append(" ");
    boolean first = true;
    for (ColumnMetadata md : nameSpace) {
      if (! first) {
        buf.append(", ");
      }
      buf.append(md.toString());
    }
    buf.append("]");
    return buf.toString();
  }
}
