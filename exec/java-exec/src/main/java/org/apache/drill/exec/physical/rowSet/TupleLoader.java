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
package org.apache.drill.exec.physical.rowSet;

import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.complex.impl.SingleMapWriter;

/**
 * Represents a tuple: either the top-level row or a nested "map" (really
 * structure) at the {@link ResultSetLoader} level. A tuple loader represents
 * loader-specific behavior, plus provides access to the tuple schema and
 * writers. (The loader is not, itself, a writer.). A tuple loader is a
 * collection of columns (backed by vectors in the loader itself.) Columns are
 * accessible via the associated tuple writer by name or index. New columns may
 * be added via this interface at any time; the new column takes the next
 * available index.
 * <p>
 * The associated writers write values into the current row or map by column
 * index or name. Column indexes and names are as defined by the schema. This
 * class wraps the basic {@link TupleWriter} with additional functionality
 * needed when writing the result set in a data reader.
 * <p>
 * Readers can define the schema up front, or can define the schema as the read
 * progresses. To avoid redundant checks to see if a column is already defined,
 * readers can simply ask for a column by name. The <tt>column()</tt> (and
 * related) methods will throw an (unchecked) {@link UndefinedColumnException}
 * exception if the column is undefined. The reader can catch the exception,
 * define the column, and fetch the column writer again.
 *
 * @see {@link SingleMapWriter}, the class which this class replaces
 */

public interface TupleLoader extends TupleWriter {

  public static final int UNMAPPED = -1;

  /**
   * Represents a column within a tuple. A column can be an array, a scalar
   * or a tuple. Each has an associated column metadata (schema) and a writer.
   * The writer is one of three kinds, depending on the kind of the column.
   * If the column is a map, then the column also has an associated tuple
   * loader to define and write to the tuple.
   * <p>
   * Every column resides at an index, is defined by a schema,
   * is backed by a value vector, and and is written to by a writer.
   * Each column also tracks the schema version in which it was added
   * to detect schema evolution. Each column has an optional overflow
   * vector that holds overflow record values when a batch becomes
   * full.
   */

  public interface ColumnLoader {
    ColumnMetadata metadata();
    TupleLoader tupleLoader();
    ObjectWriter writer();

    /**
     * Columns can be defined, but not projected into the output row. This
     * method return true if the column is  projected. Only top-level columns
     * can be omitted from the projection.
     *
     * @return true if the column is selected (data is collected),
     * false if the column is unselected (data is discarded)
     */

    boolean isProjected();
    int vectorIndex();
  }

  /**
   * Create the tuple schema from a batch schema. The tuple schema
   * must be empty.
   * @param schema the schema for the tuple
   */

  void setSchema(BatchSchema schema);

  ColumnLoader addColumn(MaterializedField field);

  /**
   * Return the column list as a batch schema. Primarily for testing.
   * @return the current schema as a batch schema, with columns
   * in the same order as they were added (that is, in row index
   * order)
   */

  BatchSchema batchSchema();

  ColumnLoader columnLoader(String name);
  ColumnLoader columnLoader(int index);
}
