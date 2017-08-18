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
package org.apache.drill.exec.physical.rowSet.impl;

import java.util.List;

import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractTupleWriter;

/**
 * Implementation of the row set loader. Provides row-level operations, leaving the
 * result set loader to provide batch-level operations. However, all control
 * operations are actually delegated to the result set loader, which handles
 * the details of working with overflow rows.
 */

public class RowSetLoaderImpl extends AbstractTupleWriter implements RowSetLoader {

  private final ResultSetLoaderImpl rsLoader;

  protected RowSetLoaderImpl(ResultSetLoaderImpl rsLoader, List<AbstractObjectWriter> writers) {
    super(rsLoader.rootModel().schema(), writers);
    this.rsLoader = rsLoader;
    bindIndex(rsLoader.writerIndex());
  }

  @Override
  public RowSetLoader setRow(Object...values) {
    if (! start()) {
      throw new IllegalStateException("Batch is full.");
    }
    setObject(values);
    save();
    return this;
  }

  @Override
  public int rowIndex() { return rsLoader.writerIndex().vectorIndex(); }

  @Override
  public void save() { rsLoader.saveRow(); }

  @Override
  public boolean start() {
    if (rsLoader.isFull()) {

      // Full batch? Return false.

      return false;
    } else if (state == State.IN_VALUE) {

      // Already in a row? Rewind the to start of the row.

      rewind();
    } else {

      // Otherwise, advance to the next row.

      rsLoader.startRow();
    }
    return true;
  }

  @Override
  public boolean isFull( ) { return rsLoader.isFull(); }

  @Override
  public void startWriteAt(int index) { }

  @Override
  public int rowCount() { return rsLoader.rowCount(); }
}
