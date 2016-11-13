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
package org.apache.drill.exec.physical.impl.xsort.managed;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.cache.VectorAccessibleSerializable;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.spill.SpillSet;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.SchemaUtil;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Stopwatch;

public class BatchGroup implements VectorAccessible, AutoCloseable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BatchGroup.class);

  private VectorContainer currentContainer;
  private SelectionVector2 sv2;
  private int pointer = 0;
  private FSDataInputStream inputStream;
  private FSDataOutputStream outputStream;
  private Path path;
  private FileSystem fs;
  private BufferAllocator allocator;
  private int spilledBatches = 0;
  private OperatorContext context;
  private BatchSchema schema;

  public BatchGroup(VectorContainer container, SelectionVector2 sv2, OperatorContext context) {
    this.sv2 = sv2;
    this.currentContainer = container;
    this.context = context;
  }

  public BatchGroup(VectorContainer container, FileSystem fs, String path, OperatorContext context) {
    currentContainer = container;
    this.fs = fs;
    this.path = new Path(path);
    this.allocator = context.getAllocator();
    this.context = context;
  }

  public SelectionVector2 getSv2() {
    return sv2;
  }

  /**
   * Updates the schema for this batch group. The current as well as any deserialized batches will be coerced to this schema
   * @param schema
   */
  public void setSchema(BatchSchema schema) {
    currentContainer = SchemaUtil.coerceContainer(currentContainer, schema, context);
    this.schema = schema;
  }

  public void addBatch(VectorContainer newContainer) throws IOException {
    assert fs != null;
    assert path != null;
    if (outputStream == null) {
      outputStream = fs.create(path);
    }
    int recordCount = newContainer.getRecordCount();
    WritableBatch batch = WritableBatch.getBatchNoHVWrap(recordCount, newContainer, false);
    VectorAccessibleSerializable outputBatch = new VectorAccessibleSerializable(batch, allocator);
    Stopwatch watch = Stopwatch.createStarted();
    outputBatch.writeToStream(outputStream);
    newContainer.zeroVectors();
    logger.debug("Took {} us to spill {} records", watch.elapsed(TimeUnit.MICROSECONDS), recordCount);
    spilledBatches++;
  }

    public void addBatch(VectorContainer newContainer) throws IOException {
      int recordCount = newContainer.getRecordCount();
      @SuppressWarnings("resource")
      WritableBatch batch = WritableBatch.getBatchNoHVWrap(recordCount, newContainer, false);
      VectorAccessibleSerializable outputBatch = new VectorAccessibleSerializable(batch, allocator);
      Stopwatch watch = Stopwatch.createStarted();
      outputBatch.writeToStream(outputStream);
      newContainer.zeroVectors();
      logger.trace("Wrote {} records in {} us", recordCount, watch.elapsed(TimeUnit.MICROSECONDS));
      spilledBatches++;

      // Hold onto the husk of the last added container so that we have a
      // current container when starting to read rows back later.

      currentContainer = newContainer;
      currentContainer.setRecordCount(0);
    }

    @Override
    public int getNextIndex() {
      if (pointer == getRecordCount()) {
        if (spilledBatches == 0) {
          return -1;
        }
        try {
          currentContainer.zeroVectors();
          getBatch();
        } catch (IOException e) {
          // Release any partially-loaded data.
          currentContainer.clear();
          throw UserException.dataReadError(e)
              .message("Failure while reading spilled data")
              .build(logger);
        }

        // The pointer indicates the NEXT index, not the one we
        // return here. At this point, we just started reading a
        // new batch and have returned index 0. So, the next index
        // is 1.

        pointer = 1;
        return 0;
      }
      return super.getNextIndex();
    }
//    logger.debug("Took {} us to read {} records", watch.elapsed(TimeUnit.MICROSECONDS), c.getRecordCount());
    spilledBatches--;
    currentContainer.zeroVectors();
    Iterator<VectorWrapper<?>> wrapperIterator = c.iterator();
    for (VectorWrapper w : currentContainer) {
      TransferPair pair = wrapperIterator.next().getValueVector().makeTransferPair(w.getValueVector());
      pair.transfer();
    }
    currentContainer.setRecordCount(c.getRecordCount());
    c.zeroVectors();
    return c;
  }

    /**
     * Close resources owned by this batch group. Each can fail; report
     * only the first error. This is cluttered because this class tries
     * to do multiple tasks. TODO: Split into multiple classes.
     */

    @Override
    public void close() throws IOException {
      IOException ex = null;
      try {
        super.close();
      } catch (IOException e) {
        ex = e;
      }
      try {
        closeOutputStream();
      } catch (IOException e) {
        ex = ex == null ? e : ex;
      }
      try {
        closeInputStream();
      } catch (IOException e) {
        ex = ex == null ? e : ex;
      }
      try {
        spillSet.delete(path);
      } catch (IOException e) {
        ex = ex == null ? e : ex;
      }
      if (ex != null) {
        throw ex;
      }
      pointer = 1;
      return 0;
    }

    private void closeInputStream() throws IOException {
      if (inputStream == null) {
        return;
      }
      long readLength = spillSet.getPosition(inputStream);
      spillSet.tallyReadBytes(readLength);
      inputStream.close();
      inputStream = null;
      logger.trace("Summary: Read {} bytes from {}", readLength, path);
    }

    public long closeOutputStream() throws IOException {
      if (outputStream == null) {
        return 0;
      }
      long posn = spillSet.getPosition(outputStream);
      spillSet.tallyWriteBytes(posn);
      outputStream.close();
      outputStream = null;
      logger.trace("Summary: Wrote {} bytes to {}", posn, path);
      return posn;
    }

    return val;
  }

  public VectorContainer getContainer() {
    return currentContainer;
  }

  @Override
  public void close() throws IOException {
    currentContainer.zeroVectors();
    if (sv2 != null) {
      sv2.clear();
    }
    if (outputStream != null) {
      outputStream.close();
    }
    if (inputStream != null) {
      inputStream.close();
    }
    if (fs != null && fs.exists(path)) {
      fs.delete(path, false);
    }
  }

  public void closeOutputStream() throws IOException {
    if (outputStream != null) {
      outputStream.close();
    }
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids) {
    return currentContainer.getValueAccessorById(clazz, ids);
  }

  @Override
  public TypedFieldId getValueVectorId(SchemaPath path) {
    return currentContainer.getValueVectorId(path);
  }

  @Override
  public BatchSchema getSchema() {
    return currentContainer.getSchema();
  }

  @Override
  public int getRecordCount() {
    if (sv2 != null) {
      return sv2.getCount();
    } else {
      return currentContainer.getRecordCount();
    }
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return currentContainer.iterator();
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    throw new UnsupportedOperationException();
  }

}
