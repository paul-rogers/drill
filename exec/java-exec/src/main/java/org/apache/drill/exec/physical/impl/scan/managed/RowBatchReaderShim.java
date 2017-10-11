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
package org.apache.drill.exec.physical.impl.scan.managed;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.impl.scan.RowBatchReader;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.record.VectorContainer;

/**
 * Represents a layer of row batch reader that works with a
 * result set loader and schema manager to structure the data
 * read by the actual row batch reader.
 */

public class RowBatchReaderShim implements RowBatchReader {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LegacyReaderFactory.class);

  private final LegacyReaderFactory manager;
  private final ManagedReader reader;
  private ResultSetLoader tableLoader;

  public RowBatchReaderShim(LegacyReaderFactory manager, ManagedReader reader) {
    this.manager = manager;
    this.reader = reader;
  }

  @Override
  public boolean open() {

    // Build and return the result set loader to be used by the reader.

    SchemaNegotiator schemaNegotiator = new SchemaNegotiatorImpl(manager.context(), this);
    if (! reader.open(schemaNegotiator)) {

      // If we had a soft failure, then there should be no schema.
      // The reader should not have negotiated one. Not a huge
      // problem, but something is out of whack.

      assert tableLoader == null;
      if (tableLoader != null) {
        logger.warn("Reader " + reader.getClass().getSimpleName() +
            " returned false from open, but negotiated a schema.");
      }
      return false;
    }

    // Storage plugins are extensible: a novice developer may not
    // have known to create the table loader. Fail in this case.

    if (tableLoader == null) {
      throw UserException.internalError(null)
        .addContext("Reader " + reader.getClass().getSimpleName() +
                    " returned true from open, but did not call SchemaNegotiator.build().")
        .build(logger);
    }
    return true;
  }

  public ResultSetLoader build(SchemaNegotiatorImpl schemaNegotiator) {
    tableLoader = manager.startFile(schemaNegotiator);
    return tableLoader;
  }

  @Override
  public boolean next() {

    // Prepare for the batch.
    // TODO: A bit wasteful to allocate vectors is the reader
    // knows it has no more data.

    tableLoader.startBatch();

    // Read the batch.

    if (! reader.next()) {

      // If the reader has no more rows, the table loader may still have
      // a lookahead row.

      if (tableLoader.writer().rowCount() == 0) {
        return false;
      }
    }

    // Have a batch. Prepare it for return.

    // Add implicit columns, if any.
    // Identify the output container and its schema version.

    manager.publish();
    return true;
  }

  @Override
  public VectorContainer output() {
    return manager.output();
  }

  @Override
  public void close() {
    RuntimeException ex = null;
    try {
      reader.close();
    } catch (RuntimeException e) {
      ex = e;
    }
    try {
      if (tableLoader != null) {
        tableLoader.close();
        tableLoader = null;
      }
    } catch (RuntimeException e) {
      ex = ex == null ? e : ex;
    }
    if (ex != null) {
      throw ex;
    }
  }

  @Override
  public int schemaVersion() {
    return tableLoader.schemaVersion();
  }
}
