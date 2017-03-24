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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
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

  /**
   * Logical description of a column. A logical column is a
   * materialized field. For maps, also includes a logical schema
   * of the map.
   */

  public static class LogicalColumn {
    public final MaterializedField field;

    /**
     * Schema of the map. Includes only those fields directly within
     * the map; does not include fields from nested tuples.
     */

    public PhysicalSchema mapSchema;

    public LogicalColumn(MaterializedField field) {
       this.field = field;
    }
  }

  /**
   * Implementation of a tuple name space. Tuples allow both indexed and
   * named access to their members.
   *
   * @param <T> the type of object representing each column
   */

  public static class NameSpace<T> {
    private final Map<String,Integer> nameSpace = new HashMap<>();
    private final List<T> columns = new ArrayList<>();

    public int add(String key, T value) {
      int index = columns.size();
      nameSpace.put(key, index);
      columns.add(value);
      return index;
    }

    public T get(int index) {
      return columns.get(index);
    }

    public T get(String key) {
      Integer index = getIndex(key);
      if (index == -1) {
        return null;
      }
      return get(index);
    }

    public int getIndex(String key) {
      Integer index = nameSpace.get(key);
      if (index == null) {
        return -1;
      }
      return index;
    }

    public int count() { return columns.size(); }
  }

  /**
   * Flattened view of the schema as needed for row-based access of scalar
   * members. The scalar view presents scalar fields: those that can be set
   * or retrieved. A separate map view presents map vectors. The scalar
   * view is the one used by row set readers and writers. Column indexes
   * are into the flattened view, with maps removed and map members flattened
   * into the top-level name space with compound names.
   */

  public static class AccessSchema {
    private final NameSpace<MaterializedField> scalars = new NameSpace<>();
    private final NameSpace<MaterializedField> maps = new NameSpace<>();

    public AccessSchema(BatchSchema schema) {
      expand("", schema);
    }

    private void expand(String prefix, Iterable<MaterializedField> fields) {
      for (MaterializedField field : fields) {
        String name = prefix + field.getName();
        if (field.getType().getMinorType() == MinorType.MAP) {
          maps.add(name, field);
          expand(name + ".", field.getChildren());
        } else {
          scalars.add(name, field);
        }
      }
    }

    /**
     * Return a column schema given an indexed into the flattened row structure.
     *
     * @param index index of the row in the flattened structure
     * @return schema of the column
     */

    public MaterializedField column(int index) { return scalars.get(index); }

    public MaterializedField column(String name) { return scalars.get(name); }

    public int columnIndex(String name) { return scalars.getIndex(name); }

    public int count() { return scalars.count(); }

    public MaterializedField map(int index) { return maps.get(index); }

    public MaterializedField map(String name) { return maps.get(name); }

    public int mapIndex(String name) { return maps.getIndex(name); }

    public int mapCount() { return maps.count(); }
  }

  /**
   * Physical schema of a row set showing the logical hierarchy of fields
   * with map fields as first-class fields. Map members appear as children
   * under the map, much as they appear in the physical value-vector
   * implementation.
   */

  public static class PhysicalSchema {
    private final NameSpace<LogicalColumn> schema = new NameSpace<>();

    public PhysicalSchema(Iterable<MaterializedField> members) {
      for (MaterializedField field : members) {
        LogicalColumn colSchema = new LogicalColumn(field);
        schema.add(field.getName(), colSchema);
        if (field.getType().getMinorType() == MinorType.MAP) {
          colSchema.mapSchema = new PhysicalSchema(field.getChildren());
        }
      }
    }

    public LogicalColumn column(int index) {
      return schema.get(index);
    }

    public LogicalColumn column(String name) {
      return schema.get(name);
    }

    public int count() { return schema.count(); }
  }

  private final BatchSchema batchSchema;
  private final AccessSchema accessSchema;
  private final PhysicalSchema physicalSchema;

  public RowSetSchema(BatchSchema schema) {
    batchSchema = schema;
    accessSchema = new AccessSchema(schema);
    physicalSchema = new PhysicalSchema(schema);
  }

  public AccessSchema access() { return accessSchema; }
  public PhysicalSchema physical() { return physicalSchema; }
  public BatchSchema batch() { return batchSchema; }

  public BatchSchema toBatchSchema(SelectionVectorMode svMode) {
    List<MaterializedField> fields = new ArrayList<>();
    for (MaterializedField field : batchSchema) {
      fields.add(field);
    }
    return new BatchSchema(svMode, fields);
  }
}
