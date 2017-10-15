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
package org.apache.drill.exec.physical.impl.scan.project;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.rowSet.ResultVectorCache;
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

  /**
   * Describes the projection of a single column from
   * an input to an output batch.
   */

  public static class Projection {
    private final VectorContainer batch;
    private final boolean direct;
    private final int fromIndex;
    private final int toIndex;
    private ValueVector fromVector;
    private ValueVector toVector;

    /**
     *
     * @param batch the source batch which contains the column to
     * be projected
     * @param direct true to directly share the same vector in the
     * output, false to copy the vector contents
     * @param fromIndex source batch index of the vector
     * @param toIndex destination batch index of the vector
     */
    public Projection(VectorContainer batch, boolean direct, int fromIndex, int toIndex) {
      assert batch != null;
      this.batch = batch;
      this.direct = direct;
      this.fromIndex = fromIndex;
      this.toIndex = toIndex;
    }

    public void setFromVector(ValueVector vector) {
      fromVector = vector;
      if (direct) {
        toVector = fromVector;
      }
    }

    public void makeToVector(VectorContainer output, ResultVectorCache vectorCache) {
      if (direct) {

        // Direct: the output vector is the input vector

        toVector = fromVector;
        output.add(toVector);
      } else if (vectorCache == null) {

        // No vector cache. Create an output vector.

        toVector = output.addOrGet(batch.getSchema().getColumn(fromIndex));
      } else {

        // Vector cache, retrieve an existing vector from
        // the cache.

        toVector = vectorCache.addOrGet(batch.getSchema().getColumn(fromIndex));
        output.add(toVector);
      }
    }

    public void project() {
      if (! direct) {
        toVector.clear();
        toVector.exchange(fromVector);
      }
    }
  }

  public static class ProjectionSet {

    private VectorContainer source;
    private List<Projection> projections = new ArrayList<>();

    public ProjectionSet(VectorContainer source) {
      this.source = source;
    }

    public void addDirectProjection(int fromIndex, int toIndex) {
      projections.add(new Projection(source, true, fromIndex, toIndex));
    }

    public void addExchangeProjection(int fromIndex, int toIndex) {
      projections.add(new Projection(source, false, fromIndex, toIndex));
    }
  }

  /**
   * Builds up the set of projections.
   */

  public static class Builder {

    private List<ProjectionSet> projectionSets = new ArrayList<>();
    private ResultVectorCache vectorCache;

    public Builder addProjectionSet(ProjectionSet projSet) {
      if (projSet != null) {
        projectionSets.add(projSet);
      }
      return this;
    }

//    public Builder addDirectProjection(VectorContainer batch, int fromIndex, int toIndex) {
//      projections.add(new Projection(batch, true, fromIndex, toIndex));
//      return this;
//    }
//
//    public Builder addExchangeProjection(VectorContainer batch, int fromIndex, int toIndex) {
//      projections.add(new Projection(batch, false, fromIndex, toIndex));
//      return this;
//    }

    public Builder vectorCache(ResultVectorCache vectorCache) {
      this.vectorCache = vectorCache;
      return this;
    }

    public RowBatchMerger build(BufferAllocator allocator) {
      return build(new VectorContainer(allocator));
    }

    public RowBatchMerger build(VectorContainer output) {
      List<Projection> projections = new ArrayList<>();
      for (ProjectionSet projSet : projectionSets) {
        projections.addAll(projSet.projections);
      }
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

      // Define column projections.

      for (int i = 0; i < count; i++) {
        Projection proj = projections.get(i);
        proj.setFromVector(proj.batch.getValueVector(proj.fromIndex).getValueVector() );
        proj.makeToVector(output, vectorCache);
      }
      output.buildSchema(SelectionVectorMode.NONE);
      Projection projArray[] = new Projection[count];
      projections.toArray(projArray);
      return new RowBatchMerger(output, projArray);
    }
  }

  private final VectorContainer output;
  private final Projection projections[];

  public RowBatchMerger(VectorContainer output, Projection projections[]) {
    this.output = output;
    this.projections = projections;
  }

  public void project(int rowCount) {
    for (int i = 0; i < projections.length; i++) {
      projections[i].project();
    }
    output.setRecordCount(rowCount);
  }

  public VectorContainer getOutput() { return output; }

  public void close() {
    output.clear();
  }
}
