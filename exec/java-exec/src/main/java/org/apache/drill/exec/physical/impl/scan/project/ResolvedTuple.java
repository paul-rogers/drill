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
import java.util.List;

import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.rowSet.ResultVectorCache;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.MapVector;

import com.google.common.annotations.VisibleForTesting;

/**
 * Drill rows are made up of a tree of tuples, with the row being the root
 * tuple. Each tuple contains columns, some of which may be maps. This
 * class represents each row or map in the output projection.
 * <p>
 * Output columns within the tuple can be projected from the data source,
 * might be null (requested columns that don't match a data source column)
 * or might be a constant (such as an implicit column.) This class
 * orchestrates assembling an output tuple from a collection of these
 * three column types. (Though implicit columns appear only in the root
 * tuple.)
 *
 * <h4>Null Handling</h4>
 *
 * The project list might reference a "missing" map if the project list
 * includes, say, <tt>SELECT a.b.c</tt> but <tt>`a`</tt> does not exist
 * in the data source. In this case, the column a is implied to be a map,
 * so the projection mechanism will create a null map for <tt>`a`</tt>
 * and <tt>`b`</tt>, and will create a null column for <tt>`c`</tt>.
 * <p>
 * To accomplish this recursive null processing, each tuple is associated
 * with a null builder. (The null builder can be null if projection is
 * implicit with a wildcard; in such a case no null columns can occur.
 * But, even here, with schema persistence, a <tt>SELECT *</tt> query
 * may need null columns if a second file does not contain a column
 * that appeared in a first file.)
 * <p>
 * The null builder is bound to each tuple to allow vector persistence
 * via the result vector cache. If we must create a null column
 * <tt>`x`</tt> in two different readers, then the rules of Drill
 * require that the same vector be used for both (or else a schema
 * change is signaled.) The vector cache works by name (and type).
 * Since maps may contain columns with the same names as other maps,
 * the vector cache must be associated with each tuple. And, by extension,
 * the null builder must also be associated with each tuple.
 *
 * <h4>Lifecycle</h4>
 *
 * The lifecycle of a resolved tuple is:
 * <ul>
 * <li>The projection mechanism creates the output tuple, and its columns,
 * by comparing the project list against the table schema. The result is
 * a set of table, null, or constant columns.</li>
 * <li>Once per schema change, the resolved tuple creates the output
 * tuple by linking to vectors in their original locations. As it turns out,
 * we can simply share the vectors; we don't need to transfer the buffers.</li>
 * <li>To prepare for the transfer, the tuple asks the null column builder
 * (if present) to build the required null columns.</li>
 * <li>Once the output tuple is built, it can be used for any number of
 * batches without further work. (The same vectors appear in the various inputs
 * and the output, eliminating the need for any transfers.)</li>
 * <li>Once per batch, the client must set the row count. This is needed for the
 * output container, and for any "null" maps that the project may have created.</li>
 * </ul>
 *
 * <h4>Projection Mapping</h4>
 *
 * Each column is is mapped into the output tuple (vector container or map) in
 * the order that the columns are defined here. (That order follows the project
 * list for explicit projection, or the table schema for implicit projection.)
 * The source, however, may be in any order (at least for the table schema.)
 * A projection mechanism identifies the {@link VectorSource} that supplies the
 * vector for the column, along with the vector's index within that source.
 * The resolved tuple is bound to an output tuple. The projection mechanism
 * grabs the input vector from the vector source at the indicated position, and
 * links it into the output tuple, represented by this projected tuple, at the
 * position of the resolved column in the child list.
 *
 * <h4>Caveats</h4>
 *
 * The project mechanism handles nested "missing" columns as mentioned
 * above. This works to create null columns within maps that are defined by the
 * data source. However, the mechanism does not currently handle creating null
 * columns within repeated maps or lists. Doing so is possible, but requires
 * adding a level of cardinality computation to create the proper number of
 * "inner" values.
 */

public abstract class ResolvedTuple implements VectorSource {

  /**
   * Represents the top-level tuple which is projected to a
   * vector container.
   */

  public static class ResolvedRowTuple extends ResolvedTuple {

    private VectorContainer input;
    private VectorContainer output;

    public ResolvedRowTuple(NullColumnBuilder nullBuilder) {
      super(nullBuilder);
    }

    public void project(VectorContainer input, VectorContainer output) {
      this.input = input;
      this.output = output;
      output.removeAll();
      buildColumns();
      output.buildSchema(SelectionVectorMode.NONE);
    }

    @Override
    public ValueVector vector(int index) {
      return input.getValueVector(index).getValueVector();
    }

    @Override
    public void addVector(ValueVector vector) {
      output.add(vector);
    }

    @Override
    public void setRowCount(int rowCount) {
      output.setRecordCount(rowCount);
      cascadeRowCount(rowCount);
    }

    @Override
    public BufferAllocator allocator() {
      return output.getAllocator();
    }

    @Override
    public String name() {

      // Never used in production, just for debugging.

       return "$root$";
    }

    public VectorContainer output() { return output; }
  }

  public static class ResolvedMapTuple extends ResolvedTuple {

    public ResolvedMapTuple(ResolvedTuple parentTuple) {
      super(parentTuple.nullBuilder == null ? null : parentTuple.nullBuilder.newChild());
    }

    @Override
    public void addVector(ValueVector vector) {
      // TODO Auto-generated method stub

    }

    @Override
    public ValueVector vector(int index) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void setRowCount(int rowCount) {
      // TODO Auto-generated method stub

    }

    @Override
    public BufferAllocator allocator() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String name() {
      // TODO Auto-generated method stub
      return null;
    }
  }

  /**
   * Represents a map implied by the project list, but which does not appear
   * in the table schema. The column is implied to be a map because it contains
   * children. This implementation builds the map and its children.
   */

  public static class NullMapTuple extends ResolvedTuple {

    private final String name;
    private final ResolvedTuple parentTuple;
    private MapVector mapVector;

    public NullMapTuple(String name, ResolvedTuple parentTuple) {
      super(parentTuple.nullBuilder == null ? null : parentTuple.nullBuilder.newChild());
      this.name = name;
      this.parentTuple = parentTuple;
      parentTuple.addChild(this);
    }

    public MapVector buildMap(String name) {
      mapVector = new MapVector(
          MaterializedField.create(name,
              Types.required(MinorType.MAP)),
          parentTuple.allocator(), null);
      buildColumns();
      return mapVector;
    }

    @Override
    public void addVector(ValueVector vector) {
      mapVector.putChild(vector.getField().getName(), vector);
    }

    @Override
    public ValueVector vector(int index) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setRowCount(int rowCount) {
      mapVector.setMapValueCount(rowCount);
      cascadeRowCount(rowCount);
    }

    @Override
    public BufferAllocator allocator() {
      return mapVector.getAllocator();
    }

    @Override
    public String name() { return name; }
  }

  protected final List<ResolvedColumn> members = new ArrayList<>();
  protected final NullColumnBuilder nullBuilder;
  protected List<ResolvedTuple> children;
  protected VectorSource binding;

  public ResolvedTuple(NullColumnBuilder nullBuilder) {
    this.nullBuilder = nullBuilder;
  }

  public NullColumnBuilder nullBuilder() {
    return nullBuilder;
  }

  public void add(ResolvedColumn col) {
    members.add(col);
  }

  public void addChild(ResolvedTuple child) {
    if (children == null) {
      children = new ArrayList<>();
    }
    children.add(child);
  }

  @VisibleForTesting
  public List<ResolvedColumn> columns() { return members; }

  public void buildNulls(ResultVectorCache vectorCache) {
    if (nullBuilder != null) {
      nullBuilder.build(vectorCache);
    }
    if (children != null) {
      for (ResolvedTuple child : children) {
        child.buildNulls(vectorCache.childCache(child.name()));
      }
    }
  }

  public void loadNulls(int rowCount) {
    if (nullBuilder != null) {
      nullBuilder.load(rowCount);
    }
    if (children != null) {

      // Just cascade the row count. This mechanism does not
      // yet support adding nulls to a repeated map vector.

      for (ResolvedTuple child : children) {
        child.loadNulls(rowCount);
      }
    }
  }

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

  protected void buildColumns() {
    for (int i = 0; i < members.size(); i++) {
      members.get(i).project(this);
    }
  }

  public abstract void addVector(ValueVector vector);

  public abstract void setRowCount(int rowCount);

  protected void cascadeRowCount(int rowCount) {
    if (children == null) {
      return;
    }
    for (ResolvedTuple child : children) {
      child.setRowCount(rowCount);
    }
  }

  public abstract BufferAllocator allocator();

  public abstract String name();

  /**
   * During planning, discard a partial plan to allow reusing the same (root) tuple
   * for multiple projection plans.
   */

  public void reset() {
    members.clear();
    children = null;
  }

  public void close() {
    if (nullBuilder != null) {
      nullBuilder.close();
    }
    if (children != null) {
      for (ResolvedTuple child : children) {
        child.close();
      }
    }
  }
}
