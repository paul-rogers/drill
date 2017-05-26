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

import org.apache.drill.exec.physical.impl.protocol.OperatorRecordBatch.OperatorExecServices;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
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
   * Setup the record reader. Called just before the first call
   * to <tt>next()</tt>. Allocate resources here, not in the constructor.
   * Example: open files, allocate buffers, etc.
   * @param context execution context
   * @param mutator row set mutator used to create batches
   */

  void open(OperatorExecServices context, ResultSetLoader mutator);

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
