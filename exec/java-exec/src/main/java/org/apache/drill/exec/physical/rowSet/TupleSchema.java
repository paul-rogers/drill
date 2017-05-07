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

import org.apache.drill.exec.record.MaterializedField;

/**
 * Defines the schema of a tuple: either the top-level row or a nested
 * "map" (really structure). A schema is a collection of columns (backed
 * by vectors in the loader itself.) Columns are accessible by name or
 * index. New columns may be added at any time; the new column takes the
 * next available index.
 */

public interface TupleSchema {
  int columnCount();
  int columnIndex(String colName);
  MaterializedField column(int colIndex);

  /**
   * Return the schema for the given column name.
   *
   * @param colName column name within the tuple as a
   * single-part name
   * @return the schema if the column is defined, null
   * otherwise
   */

  MaterializedField column(String colName);

  /**
   * Add a new column to the schema.
   *
   * @param columnSchema
   * @return the index of the new column
   */

  int addColumn(MaterializedField columnSchema);
}
