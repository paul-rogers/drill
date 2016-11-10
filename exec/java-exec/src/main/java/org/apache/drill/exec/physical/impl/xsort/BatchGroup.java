/**
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
package org.apache.drill.exec.physical.impl.xsort;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.cache.VectorAccessibleSerializable;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.OperatorContext;
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

/**
 * Represents a group of batches spilled to disk.
 * <p>
 * The batches are defined by a schema which can change over time. When the schema changes,
 * all existing and new batches are coerced into the new schema. Provides a
 * uniform way to iterate over records for one or more batches whether
 * the batches are in memory or on disk.
 * <p>
 * The <code>BatchGroup</code> operates in two modes as given by the two
 * subclasses:
 * <ul>
 * <li>Input mode (@link InputBatchGroup): Used to buffer in-memory batches
 * prior to spilling.</li>
 * <li>Spill mode (@link SpilledBatchGroup): Holds a "memento" to a set
 * of batches written to disk. Acts as both a reader and writer for
 * those batches.</li>
 */

public abstract class BatchGroup implements VectorAccessible, AutoCloseable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BatchGroup.class);

  /**
   * The input batch group gathers batches buffered in memory before
   * spilling. The structure of the data is:
   * <ul>
   * <li>Contains a single batch received from the upstream (input)
   * operator.</li>
   * <li>Associated selection vector that provides a sorted
   * indirection to the values in the batch.</li>
   * </ul>
   */

  public static class InputBatchGroup extends BatchGroup {
    private SelectionVector2 sv2;
    private int batchSize;

    public InputBatchGroup(VectorContainer container, SelectionVector2 sv2, OperatorContext context, int batchSize) {
      super( container, context );
      this.sv2 = sv2;
      this.batchSize = batchSize;
    }

    public SelectionVector2 getSv2() {
      return sv2;
    }

    @Override
    public int getRecordCount() {
      if (sv2 != null) {
        return sv2.getCount();
      } else {
        return super.getRecordCount();
      }
    }
    
    public int getBatchSize( ) { return batchSize; }

    @Override
    public int getNextIndex() {
      int val = super.getNextIndex();
      if ( val == -1 ) {
        return val;
      }
      return sv2.getIndex(val);
    }

    @Override
    public void close() throws IOException {
      super.close( );
      if (sv2 != null) {
        sv2.clear();
      }
    }
  }

  /**
   * Holds a set of spilled batches, represented by a file on disk.
   * Handles reads from, and writes to the spill file. The data structure
   * is:
   * <ul>
   * <li>A pointer to a file that contains serialized batches.</li>
   * <li>When writing, each batch is appended to the output file.</li>
   * <li>When reading, iterates over each spilled batch, and for each
   * of those, each spilled record.</li>
   * </ul>
   * <p>
   * Starts out with no current batch. Defines the current batch to be the
   * (shell: schema without data) of the last batch spilled to disk.
   * <p>
   * When reading, has destructive read-once behavior: closing the
   * batch (after reading) deletes the underlying spill file.
   */

  public static class SpilledBatchGroup extends BatchGroup {
    private FSDataInputStream inputStream;
    private FSDataOutputStream outputStream;
    private Path path;
    private FileSystem fs;
    private BufferAllocator allocator;
    private int spilledBatches = 0;

    public SpilledBatchGroup(FileSystem fs, String path, OperatorContext context) {
      super( null, context );
      this.fs = fs;
      this.path = new Path(path);
      this.allocator = context.getAllocator();
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
          throw new RuntimeException(e);
        }
        pointer = 1;
        return 0;
      }
      return super.getNextIndex();
    }

    private VectorContainer getBatch() throws IOException {
      assert fs != null;
      assert path != null;
      if (inputStream == null) {
        inputStream = fs.open(path);
      }
      VectorAccessibleSerializable vas = new VectorAccessibleSerializable(allocator);
      Stopwatch watch = Stopwatch.createStarted();
      vas.readFromStream(inputStream);
      VectorContainer c =  vas.get();
      if (schema != null) {
        c = SchemaUtil.coerceContainer(c, schema, context);
      }
//      logger.debug("Took {} us to read {} records", watch.elapsed(TimeUnit.MICROSECONDS), c.getRecordCount());
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

    @Override
    public void close() throws IOException {
      super.close( );
      closeOutputStream( );
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
        outputStream = null;
      }
    }
  }

  protected VectorContainer currentContainer;
  protected int pointer = 0;
  protected OperatorContext context;
  protected BatchSchema schema;

  public BatchGroup(VectorContainer container, OperatorContext context) {
    this.currentContainer = container;
    this.context = context;
  }

  /**
   * Updates the schema for this batch group. The current as well as any 
   * deserialized batches will be coerced to this schema.
   * @param schema
   */
  public void setSchema(BatchSchema schema) {
    currentContainer = SchemaUtil.coerceContainer(currentContainer, schema, context);
    this.schema = schema;
  }

  public int getNextIndex() {
    if (pointer == getRecordCount()) {
      return -1;
    }
    int val = pointer++;
    assert val < currentContainer.getRecordCount();
    return val;
  }

  public VectorContainer getContainer() {
    return currentContainer;
  }

  @Override
  public void close() throws IOException {
    currentContainer.zeroVectors();
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
    return currentContainer.getRecordCount();
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
