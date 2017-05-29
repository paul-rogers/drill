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

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.physical.impl.protocol.OperatorRecordBatch.OperatorExecServices;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.RecordReader;

/**
 * Extended version of a record reader which uses a size-aware
 * batch mutator. Use this for all new readers. Replaces the
 * original {@link RecordReader} interface.
 * <p>
 * Note that this interface reads a <b>batch</b> of rows, not
 * a single row. (The original <tt>RecordReader</tt> could be
 * confusing in this aspect.)
 */

public interface RowBatchReader {

  /**
   * Negotiate the select and table schemas with the scan operator. Depending
   * on the design of the storage plugin, the selection list may be something that
   * the scan operator understands, or that the record reader understands. If the
   * storage plugin; the <tt>addSelectColumn()</tt> methods are not needed. But,
   * if the record reader defines the select list, call the various
   * <tt>addSelectColumn()</tt> methods to specify the select. All methods are
   * equivalent, though processing can be saved if the reader knows the column
   * type.
   * <p>
   * All readers must announce the table schema. This can be done at open time
   * for an "early schema" reader. But, if the schema is not known at open
   * time, then the reader is a "late schema" reader and schema will be
   * discovered, and adjusted, on each batch.
   * <p>
   * Regardless of the schema type, the result of building the schema is a
   * result set loader used to prepare batches for use in the query. If the
   * select list contains a subset of columns from the table, then the result
   * set loader will return null when the reader asks for the column loader for
   * that column. The null value tells the reader to skip that column. The reader
   * can use that information to avoid reading the data, if possible, for
   * efficiency.
   */

  public interface SchemaNegotiator {
    enum ColumnType { ANY, TABLE, META_ANY, META_IMPLICIT, META_PARTITION };

    void addSelectColumn(String name);
    void addSelectColumn(SchemaPath path);
    void addSelectColumn(SchemaPath path, ColumnType type);
    void addTableColumn(String name, MajorType type);
    void addTableColumn(MaterializedField schema);
    ResultSetLoader build();
    // TODO: return a projection map as an array of booleans
  }

  /**
   * Setup the record reader. Called just before the first call
   * to <tt>next()</tt>. Allocate resources here, not in the constructor.
   * Example: open files, allocate buffers, etc.
   * @param context execution context
   * @param schemaNegotiator mechanism to negotiate select and table
   * schemas, then create the row set reader used to load data into
   * value vectors
   */

  void open(OperatorExecServices context, SchemaNegotiator schemaNegotiator);

  /**
   * Read the next batch. Reading continues until either EOF,
   * or until the mutator indicates that the batch is full.
   * @return true if the current batch is valid, false if the
   * batch is empty and no more batches are available to read
   */

  boolean next();

  /**
   * Release resources. Called just after a failure, when the scanner
   * is cancelled, or after <tt>next()</tt> returns EOF. Release
   * all resources and close files.
   */
  void close();
}
