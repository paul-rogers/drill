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
import org.apache.drill.common.types.TypeProtos.MinorType;

/**
 * Metadata description of the schema of a row or a map.
 * In Drill, both rows and maps are
 * tuples: both are an ordered collection of values, defined by a
 * schema. Each tuple has a schema that defines the column ordering
 * for indexed access. Each tuple also provides methods to get column
 * accessors by name or index.
 * <p>
 * Models the physical schema of a row set showing the logical hierarchy of fields
 * with map fields as first-class fields. Map members appear as children
 * under the map, much as they appear in the physical value-vector
 * implementation.
 * <ul>
 * <li>Provides fast lookup by name or index.</li>
 * <li>Provides a nested schema, in this same form, for maps.</li>
 * </ul>
 * This form is useful when performing semantic analysis and when
 * working with vectors.
 * <p>
 * In the future, this structure will also gather metadata useful
 * for vector processing such as expected widths and so on.
 */

public class TupleMetadata implements Iterable<TupleMetadata.ColumnMetadata> {

  public enum StructureType {
    PRIMITIVE, LIST, TUPLE
  }

  public static abstract class ColumnMetadata {
    private final int index;
    private final TupleMetadata parent;
    protected final MaterializedField schema;
    protected final ColumnMetadata base;

    public ColumnMetadata(int index, TupleMetadata parent, MaterializedField schema, ColumnMetadata base) {
      this.index = index;
      this.parent = parent;
      this.schema = schema;
      this.base = base;
    }

    public abstract StructureType structureType();
    public abstract TupleMetadata mapSchema();
    public int index() { return index; }
    public MaterializedField schema() { return schema; }
    public String name() { return schema.getName(); }
    public MinorType type() { return schema.getType().getMinorType(); }
    public DataMode mode() { return schema.getDataMode(); }
    public TupleMetadata parent() { return parent; }
    public MapColumnMetadata parentMap() { return parent.map(); }
    public ColumnMetadata base() { return base; }

    /**
     * Full name of the column. Note: this name cannot be used to look up
     * the column because of ambiguity. The name "a.b.c" may mean a single
     * column with that name, or may mean maps "a", and "b" with column "c",
     * etc.
     *
     * @return full, dotted, column name
     */

    public String fullName( ) {
      MapColumnMetadata parentMap = parentMap();
      if (parentMap == null) {
        return name();
      } else {
        return parentMap.fullName() + "." + name();
      }
    }

    public boolean isEquivalent(ColumnMetadata other) {
      return schema.isEquivalent(other.schema);
    }
  }

  public static class PrimitiveColumnMetadata extends ColumnMetadata {

    public PrimitiveColumnMetadata(int index, TupleMetadata parent,
                                   MaterializedField schema, PrimitiveColumnMetadata base) {
      super(index, parent, schema, base);
    }

    @Override
    public StructureType structureType() { return StructureType.PRIMITIVE; }
    @Override
    public TupleMetadata mapSchema() { return null; }
  }

  public static class MapColumnMetadata  extends ColumnMetadata {
    private final TupleMetadata mapSchema;

    public MapColumnMetadata(int index, TupleMetadata parent, MaterializedField schema, MapColumnMetadata base) {
      super(index, parent, schema, base);
      mapSchema = new TupleMetadata(this);
      for (MaterializedField child : schema.getChildren()) {
        mapSchema.add(child);
      }
    }

    @Override
    public StructureType structureType() { return StructureType.TUPLE; }
    @Override
    public TupleMetadata mapSchema() { return mapSchema; }
  }

  private final MapColumnMetadata parentMap;
  private final TupleNameSpace<ColumnMetadata> nameSpace = new TupleNameSpace<>();

  public TupleMetadata() { this((MapColumnMetadata) null); }

  public TupleMetadata(MapColumnMetadata parentMap) {
    this.parentMap = parentMap;
  }

  public static TupleMetadata fromFields(MapColumnMetadata parent, Iterable<MaterializedField> fields) {
    TupleMetadata tuple = new TupleMetadata(parent);
    for (MaterializedField field : fields) {
      tuple.add(field);
    }
    return tuple;
  }

  public static TupleMetadata fromFields(Iterable<MaterializedField> fields) {
    return fromFields(null, fields);
  }

  public static TupleMetadata copy(TupleMetadata schema) {
    TupleMetadata tuple = new TupleMetadata();
    for (ColumnMetadata md : schema) {
      tuple.add(md.schema());
    }
    return tuple;
  }

  public void add(MaterializedField field) {
    addColumn(field, null);
  }

  public void addReference(ColumnMetadata base) {
    addColumn(base.schema(), base);
  }

  private void addColumn(MaterializedField field, ColumnMetadata base) {
    int index = nameSpace.count();
    ColumnMetadata md;
    if (field.getType().getMinorType() == MinorType.MAP) {
      md = new MapColumnMetadata(index, this, field, (MapColumnMetadata) base);
    } else {
      md = new PrimitiveColumnMetadata(index, this, field, (PrimitiveColumnMetadata) base);
    }
    nameSpace.add(field.getName(), md);
  }

  public MaterializedField column(String name) {
    ColumnMetadata md = metadata(name);
    return md == null ? null : md.schema();
  }

  public ColumnMetadata metadata(String name) {
    return nameSpace.get(name);
  }

  public int index(String name) {
    return nameSpace.indexOf(name);
  }

  public MaterializedField column(int index) {
    ColumnMetadata md = metadata(index);
    return md == null ? null : md.schema();
  }

  public ColumnMetadata metadata(int index) {
    return nameSpace.get(index);
  }

  public MapColumnMetadata map() { return parentMap; }
  public int size() { return nameSpace.count(); }

  public boolean isEmpty() { return nameSpace.count( ) == 0; }

  @Override
  public Iterator<ColumnMetadata> iterator() {
    return nameSpace.iterator();
  }

  public boolean isEquivalent(TupleMetadata other) {
    if (nameSpace.count() != other.nameSpace.count()) {
      return false;
    }
    for (int i = 0; i < nameSpace.count(); i++) {
      if (! nameSpace.get(i).isEquivalent(other.nameSpace.get(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return nameSpace.toString();
  }

  public TupleMetadata merge(TupleMetadata other) {
    TupleMetadata merged = copy(this);
    for (ColumnMetadata md : other) {
      merged.add(md.schema());
    }
    return merged;
  }

  /**
   * Create a flattened form of this schema with non-repeated maps
   * flattened into the top schema. Repeated maps continue to appear
   * as nested maps.
   * <p>
   * Note that this form of the schema is useful only for tests in
   * which the test guarantees name uniqueness. Does not work in the
   * general case because the path "a"."b.c" will collide with
   * "a"."b"."c", "a.b"."c" and "a.b.c".
   * <p>
   * The returned schema is a left-to-right, depth-first traversal of the row and map
   * columns. Holds only materialized vectors (non-maps).
   * <p>
   * A special case occurs for lists and repeated maps. In this case, a single
   * column cannot capture the repetition semantics. For these cases, the
   * column here represents an array. The Array content is another array
   * (for a list) of a column, or is a tuple.
   * <p>
   * Examples:
   * <li>
   * <li>Map: Flattened into the row schema; map columns appear as top-level
   * columns.</li>
   * <li>Int: Appears in the row schema.</li>
   * <li>Int Array: Appears in the row schema because there is no ambiguity.
   * <li>
   * <li>List of maps: A list column appears in the top-schema and represents
   * the list. The list content is a tuple representing the map. Map columns
   * are available on the (nested) tuple.</li>
   * <li>List of list of ints (AKA list of int arrays): The top-level field
   * is a list, with the entry as an int array.</li>
   * </ul>
   *
   * @return flattened version of this schema
   */

  public TupleMetadata flatten() {
    TupleMetadata flatTuple = new TupleMetadata();
    flattenTo(flatTuple);
    return flatTuple;
  }

  private void flattenTo(TupleMetadata flatTuple) {
    for (ColumnMetadata md : nameSpace) {
      if (md.type() != MinorType.MAP) {
        flatTuple.addReference(md);
      } else if (md.mode() == DataMode.REPEATED) {

      } else {
        MapColumnMetadata mapMd = (MapColumnMetadata) md;
        mapMd.mapSchema().flattenTo(flatTuple);
      }
    }
  }

  /**
   * Return the schema as a list of <tt>MaterializedField</tt> objects
   * which can be used to create other schemas. Not valid for a
   * flattened schema.
   *
   * @return a list of the top-level fields. Maps contain their child
   * fields
   */

  public List<MaterializedField> toFieldList() {
    List<MaterializedField> cols = new ArrayList<>();
    for (ColumnMetadata md : nameSpace) {
      cols.add(md.schema());
    }
    return cols;
  }
}
