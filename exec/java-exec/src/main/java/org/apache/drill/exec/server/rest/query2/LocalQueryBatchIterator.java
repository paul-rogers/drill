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
package org.apache.drill.exec.server.rest.query2;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.materialize.QueryWritableBatch;
import org.apache.drill.exec.physical.resultSet.impl.PullResultSetReaderImpl.UpstreamSource;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.selection.SelectionVector2;

public class LocalQueryBatchIterator implements UpstreamSource {

  public interface BatchSource {
    QueryWritableBatch next();
  }

  private final BatchSource source;
  private final RecordBatchLoader loader;
  private int schemaVersion;
  private int recordCount;
  private int batchCount;

  public LocalQueryBatchIterator(BufferAllocator allocator, BatchSource source) {
    this.source = source;
    this.loader = new RecordBatchLoader(allocator);
  }

  @Override
  public boolean next() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int schemaVersion() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public VectorContainer batch() {
    QueryWritableBatch writableBatch = source.next();
    if (writableBatch == null) {
      return null;
    }
    batchCount++;
    recordCount += writableBatch.getHeader().getRowCount();

    // Unload the batch and convert to a row set.
//    loader.load(writableBatch.getHeader().getDef(), writableBatch.getBuffers());
//    inputBatch.release();
//    VectorContainer batch = loader.getContainer();
//    batch.setRecordCount(loader.getRecordCount());
//
//    // Null results? Drill will return a single batch with no rows
//    // and no columns even if the scan (or other) operator returns
//    // no batches at all. For ease of testing, simply map this null
//    // result set to a null output row set that says "nothing at all
//    // was returned." Note that this is different than an empty result
//    // set which has a schema, but no rows.
//    if (batch.getRecordCount() == 0 && batch.getNumberOfColumns() == 0) {
//      release();
//      return false;
//    }
//
//    if (state == State.START || batch.isSchemaChanged()) {
//      schemaVersion++;
//    }
//    state = State.RUN;
//    return true;
    return null;
  }

  @Override
  public SelectionVector2 sv2() { return null; }

  @Override
  public void release() {
    // TODO Auto-generated method stub

  }
}
