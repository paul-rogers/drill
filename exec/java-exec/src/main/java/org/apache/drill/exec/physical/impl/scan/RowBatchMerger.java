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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;

/**
 * Merge two or more batches to produce an output batch. For example, consider
 * two input batches:<pre><code>
 * (a, d, e)
 * (c, b)</code></pre>
 * We may wish to merge them by projecting columns into an output batch
 * of the form:<pre><code>
 * (a, b, c, d)</code></pre>
 * It is not necessary to project all columns from the inputs, but all
 * columns in the output must have a projection.
 * <p>
 * The merger is created once per schema, then can be reused for any
 * number of batches. The only restriction is that the two batches must
 * have the same row count so that the output makes sense.
 * <p>
 * Merging is done by discarding any data in the output, then exchanging
 * the buffers from the input columns to the output, leaving projected
 * columns empty. Note that unprojected columns must be cleared by the
 * caller.
 */

public class RowBatchMerger {

  public static class Projection {
    private final VectorContainer batch;
    private final int fromIndex;
    private final int toIndex;

    public Projection(VectorContainer batch, int fromIndex, int toIndex) {
      this.batch = batch;
      this.fromIndex = fromIndex;
      this.toIndex = toIndex;
    }
  }

  public static class Builder {

    private List<Projection> projections = new ArrayList<>();

    public Builder addBatch(VectorContainer batch, int projection[]) {
      for (int i = 0; i < projection.length; i++) {
        if (projection[i] != -1) {
          addProjection(batch, i, projection[i]);
        }
      }
      return this;
    }

    public Builder addProjection(VectorContainer batch, int fromIndex, int toIndex) {
      projections .add(new Projection(batch, fromIndex, toIndex));
      return this;
    }

    public RowBatchMerger build(BufferAllocator allocator) {
      return build(new VectorContainer(allocator));
    }

    public RowBatchMerger build(VectorContainer output) {
      Collections.sort(projections, new Comparator<Projection>() {

        @Override
        public int compare(Projection o1, Projection o2) {
           return Integer.compare(o1.toIndex, o2.toIndex);
        }
      });

      // Sanity check

      int count = projections.size();
      for (int i = 0; i < count; i++) {
        if (projections.get(i).toIndex != i) {
          throw new IllegalArgumentException("Missing projection at index " + i);
        }
      }

      ValueVector inputs[] = new ValueVector[count];
      ValueVector outputs[] = new ValueVector[count];
      for (int i = 0; i < count; i++) {
        Projection proj = projections.get(i);
        inputs[i] = proj.batch.getValueVector(proj.fromIndex).getValueVector();
        outputs[i] = output.addOrGet(proj.batch.getSchema().getColumn(proj.fromIndex));
      }
      output.buildSchema(SelectionVectorMode.NONE);
      return new RowBatchMerger(output, inputs, outputs);
    }
  }

  private final VectorContainer output;
  private final ValueVector sourceVectors[];
  private final ValueVector destVectors[];

  public RowBatchMerger(VectorContainer output, ValueVector[] inputs,
      ValueVector[] outputs) {
    this.output = output;
    sourceVectors = inputs;
    destVectors = outputs;
  }

  public void project(int rowCount) {
    output.zeroVectors();
    for (int i = 0; i < sourceVectors.length; i++) {
      sourceVectors[i].exchange(destVectors[i]);
    }
    output.setRecordCount(rowCount);
  }

  public VectorContainer getOutput() { return output; }
}
