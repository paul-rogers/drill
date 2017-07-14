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
package org.apache.drill.test.rowSet;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.impl.TupleNameSpace;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.vector.accessor.TupleAccessor.TupleSchema;
import org.apache.drill.exec.record.MaterializedField;

/**
 * Row set schema presented as a number of distinct "views" for various
 * purposes:
 * <ul>
 * <li>Batch schema: the schema used by a VectorContainer.</li>
 * <li>Physical schema: the schema expressed as a hierarchy of
 * tuples with the top tuple representing the row, nested tuples
 * representing maps.</li>
 * <li>Access schema: a flattened schema with all scalar columns
 * at the top level, and with map columns pulled out into a separate
 * collection. The flattened-scalar view is the one used to write to,
 * and read from, the row set.</li>
 * </ul>
 * Allows easy creation of multiple row sets from the same schema.
 * Each schema is immutable, which is fine for tests in which we
 * want known inputs and outputs.
 */

public class RowSetSchema {

  public enum LogicalType {
    PRIMITIVE, LIST, TUPLE
  }

  /**
   * Logical description of a column. A logical column is a
   * materialized field. For maps, also includes a logical schema
   * of the map.
   */

  public static class LogicalColumn {
    protected final String fullName;
    protected final int accessIndex;
    protected int flatIndex;
    protected final MaterializedField field;
    protected final LogicalType logicalType;

    /**
     * Schema of the map. Includes only those fields directly within
     * the map; does not include fields from nested tuples.
     */

    protected PhysicalSchema mapSchema;

    protected LogicalColumn elementSchema;

    public LogicalColumn(String fullName, int accessIndex, MaterializedField field) {
      this.fullName = fullName;
      this.accessIndex = accessIndex;
      this.field = field;
      switch (field.getType().getMinorType()) {
      case LIST:
        logicalType = LogicalType.LIST;
        break;
      case MAP:
        if (field.getType().getMode() == DataMode.REPEATED) {
          logicalType = LogicalType.LIST;
        } else {
          logicalType = LogicalType.TUPLE;
        }
        break;
      default:
        logicalType = LogicalType.PRIMITIVE;
      }
    }

    private void updateStructure(int index, PhysicalSchema children) {
      flatIndex = index;
      mapSchema = children;
    }

    public int accessIndex() { return accessIndex; }
    public int flatIndex() { return flatIndex; }
    public boolean isMap() { return logicalType == LogicalType.TUPLE; }
    public boolean isList() { return logicalType == LogicalType.LIST; }
    public PhysicalSchema mapSchema() { return mapSchema; }
    public LogicalColumn elementSchema() { return elementSchema; }
    public MaterializedField field() { return field; }
    public String fullName() { return fullName; }
  }

  /**
   * Provides a non-flattened, physical view of the schema. The top-level
   * row includes maps, maps expand to a nested tuple schema. This view
   * corresponds, more-or-less, to the physical storage of vectors in
   * a vector accessible or vector container.
   */

  private static class TupleSchemaImpl implements TupleSchema {

    private TupleNameSpace<LogicalColumn> columns;

    public TupleSchemaImpl(TupleNameSpace<LogicalColumn> ns) {
      this.columns = ns;
    }

    @Override
    public MaterializedField column(int index) {
      return logicalColumn(index).field();
    }

    public LogicalColumn logicalColumn(int index) { return columns.get(index); }

    @Override
    public MaterializedField column(String name) {
      LogicalColumn col = columns.get(name);
      return col == null ? null : col.field();
    }

    @Override
    public int columnIndex(String name) {
      return columns.indexOf(name);
    }

    @Override
    public int count() { return columns.count(); }
  }

  /**
   * Represents the flattened view of the schema used to get and set columns.
   * Represents a left-to-right, depth-first traversal of the row and map
   * columns. Holds only materialized vectors (non-maps). For completeness,
   * provides access to maps also via separate methods, but this is generally
   * of little use.
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
   */

  public static class FlattenedSchema extends TupleSchemaImpl {
    protected final TupleSchemaImpl maps;

    public FlattenedSchema(TupleNameSpace<LogicalColumn> cols, TupleNameSpace<LogicalColumn> maps) {
      super(cols);
      this.maps = new TupleSchemaImpl(maps);
    }

    public LogicalColumn logicalMap(int index) { return maps.logicalColumn(index); }
    public MaterializedField map(int index) { return maps.column(index); }
    public MaterializedField map(String name) { return maps.column(name); }
    public int mapIndex(String name) { return maps.columnIndex(name); }
    public int mapCount() { return maps.count(); }
  }

  /**
   * Physical schema of a row set showing the logical hierarchy of fields
   * with map fields as first-class fields. Map members appear as children
   * under the map, much as they appear in the physical value-vector
   * implementation.
   */

  public static class PhysicalSchema {
    protected final TupleNameSpace<LogicalColumn> schema = new TupleNameSpace<>();

    public LogicalColumn column(int index) {
      return schema.get(index);
    }

    public LogicalColumn column(String name) {
      return schema.get(name);
    }

    public int count() { return schema.count(); }

    public TupleNameSpace<LogicalColumn> nameSpace() { return schema; }
  }

  private static class SchemaExpander {
    private final PhysicalSchema physicalSchema;
    private final TupleNameSpace<LogicalColumn> cols = new TupleNameSpace<>();
    private final TupleNameSpace<LogicalColumn> maps = new TupleNameSpace<>();

    public SchemaExpander(BatchSchema schema) {
      physicalSchema = expand("", schema);
    }

    private PhysicalSchema expand(String prefix, Iterable<MaterializedField> fields) {
      PhysicalSchema physical = new PhysicalSchema();
      for (MaterializedField field : fields) {
        String name = prefix + field.getName();
        int index;
        LogicalColumn colSchema = new LogicalColumn(name, physical.count(), field);
        physical.schema.add(field.getName(), colSchema);
        PhysicalSchema children = null;
        if (field.getType().getMinorType() == MinorType.MAP) {
          index = maps.add(name, colSchema);
          children = expand(name + ".", field.getChildren());
        } else {
          index = cols.add(name, colSchema);
        }
        colSchema.updateStructure(index, children);
      }
      return physical;
    }
  }

  private final BatchSchema batchSchema;
  private final TupleSchemaImpl accessSchema;
  private final FlattenedSchema flatSchema;
  private final PhysicalSchema physicalSchema;

  public RowSetSchema(BatchSchema schema) {
    batchSchema = schema;
    SchemaExpander expander = new SchemaExpander(schema);
    physicalSchema = expander.physicalSchema;
    accessSchema = new TupleSchemaImpl(physicalSchema.nameSpace());
    flatSchema = new FlattenedSchema(expander.cols, expander.maps);
  }

  /**
   * A hierarchical schema that includes maps, with maps expanding
   * to a nested tuple schema. Not used at present; this is intended
   * to be the bases of non-flattened accessors if we find the need.
   * @return the hierarchical access schema
   */

  public TupleSchema hierarchicalAccess() { return accessSchema; }

  /**
   * A flattened (left-to-right, depth-first traversal) of the non-map
   * columns in the row. Used to define the column indexes in the
   * get methods for row readers and the set methods for row writers.
   * @return the flattened access schema
   */

  public FlattenedSchema flatAccess() { return flatSchema; }

  /**
   * Internal physical schema in hierarchical order. Mostly used to create
   * the other schemas, but may be of use in special cases. Has the same
   * structure as the batch schema, but with additional information.
   * @return a tree-structured physical schema
   */

  public PhysicalSchema physical() { return physicalSchema; }

  /**
   * The batch schema used by the Drill runtime. Represents a tree-structured
   * list of top-level fields, including maps. Maps contain a nested schema.
   * @return the batch schema used by the Drill runtime
   */

  public BatchSchema batch() { return batchSchema; }

  /**
   * Convert this schema to a new batch schema that includes the specified
   * selection vector mode.
   * @param svMode selection vector mode for the new schema
   * @return the new batch schema
   */

  public BatchSchema toBatchSchema(SelectionVectorMode svMode) {
    List<MaterializedField> fields = new ArrayList<>();
    for (MaterializedField field : batchSchema) {
      fields.add(field);
    }
    return new BatchSchema(svMode, fields);
  }
}
