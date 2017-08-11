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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.record.TupleMetadata.StructureType;
import org.apache.drill.exec.record.TupleSchema;
import org.apache.drill.exec.record.TupleSchema.AbstractColumnMetadata;
import org.apache.drill.exec.vector.accessor.ObjectWriter;

/**
 * Shim inserted between an actual tuple loader and the client to remove columns
 * that are not projected from input to output. The underlying loader handles only
 * the projected columns in order to improve efficiency. This class presents the
 * full table schema, but returns null for the non-projected columns. This allows
 * the reader to work with the table schema as defined by the data source, but
 * skip those columns which are not projected. Skipping non-projected columns avoids
 * creating value vectors which are immediately discarded. It may also save the reader
 * from reading unwanted data.
 */
public class LogicalTupleLoader extends AbstractTupleLoader {

  /**
   * Represents a column from the input source that is projected to
   * a column in the output result set. Projected columns have a
   * writer to do the actual writing. Only primitive (or arrays of
   * primitives) are projected.
   */

  private static class ProjectedColumn extends AbstractColumnLoader {

    /**
     * Physical column loader to which this logical column
     * projects.
     */

    private final BaseColumnLoader mapping;

    public ProjectedColumn(LogicalTupleLoader parentTuple, ColumnMetadata schema, BaseColumnLoader mapping) {
      super(parentTuple, schema);
      this.mapping = mapping;
    }

    @Override
    public int vectorIndex() { return mapping.vectorIndex(); }

    @Override
    public ObjectWriter writer() { return mapping.writer(); }

    @Override
    public void startBatch() {
      mapping.startBatch();
    }

    @Override
    public void rollOver(int overflowIndex) {
      mapping.rollOver(overflowIndex);
    }

    @Override
    public void harvest() {
      mapping.harvest();
    }

    @Override
    public void reset() {
      mapping.reset();
    }

    @Override
    public void buildContainer(VectorContainerBuilder containerBuilder) {
      mapping.buildContainer(containerBuilder);
    }
  }

  /**
   * Input column that is <b>not</i> projected to the output result set.
   * Such columns have no associated writer.
   */

  private static class NonProjectedColumn extends AbstractColumnLoader {

    public NonProjectedColumn(LogicalTupleLoader parentTuple,
        ColumnMetadata schema) {
      super(parentTuple, schema);
    }

    @Override
    public boolean isProjected() { return false; }

    @Override
    public int vectorIndex() { return UNMAPPED; }

    @Override
    public ObjectWriter writer() { return null; }

    @Override
    public void startBatch() { }

    @Override
    public void rollOver(int overflowIndex) { }

    @Override
    public void harvest() { }

    @Override
    public void reset() { }

    @Override
    public void buildContainer(VectorContainerBuilder containerBuilder) { }
  }

  /**
   * Specialized version of column metadata which presents a physical column
   * as a logical column. All information is passed through except that the
   * column index is the logical index, which is generally different than the
   * physical index.
   */
  private static class ProjectedColumnMetadata extends AbstractColumnMetadata {

    private final TupleMetadata parentTuple;
    private final int index;
    private final ColumnMetadata physicalColumn;

    public ProjectedColumnMetadata(TupleMetadata parentTuple, ColumnMetadata physicalColumn, int index) {
      this.parentTuple = parentTuple;
      this.index = index;
      this.physicalColumn = physicalColumn;
    }
    @Override
    public StructureType structureType() { return physicalColumn.structureType(); }

    @Override
    public TupleMetadata mapSchema() { return physicalColumn.mapSchema(); }

    @Override
    public int index() { return index; }

    @Override
    public MaterializedField schema() { return physicalColumn.schema(); }

    @Override
    public String name() { return physicalColumn.name(); }

    @Override
    public TupleMetadata parent() { return parentTuple; }

    @Override
    public int expectedWidth() { return physicalColumn.expectedWidth(); }
  }

  /**
   * Set of names of projected columns.
   */

  private final Set<String> selection = new HashSet<>();

  /**
   * The physical tuple loader behind this logical presentation.
   */

  private final AbstractTupleLoader physicalLoader;

  public LogicalTupleLoader(ResultSetLoaderImpl rsLoader, AbstractTupleLoader physicalLoader, Collection<String> selection) {
    super(rsLoader);
    this.physicalLoader = physicalLoader;
  }

  @Override
  public void setSchema(BatchSchema schema) {
    if (schema().size() != 0) {
      throw new IllegalStateException("Can only set schema when the tuple schema is empty");
    }
    for (MaterializedField field : schema) {
      addColumn(field);
    }
  }

  @Override
  public ColumnLoader addColumn(MaterializedField columnSchema) {
    String key = resultSetLoader().toKey(columnSchema.getName());
    AbstractColumnLoader colLoader;
    if (columnSchema.getType().getMinorType() == MinorType.MAP ||
        selection.contains(key)) {
      BaseColumnLoader physicalColLoader = (BaseColumnLoader) physicalLoader.addColumn(columnSchema);
      ColumnMetadata columnMetadata = new ProjectedColumnMetadata(schema, physicalColLoader.metadata(), schema.size());
      ((TupleSchema) schema).add(columnMetadata);
      colLoader = new ProjectedColumn(this, columnMetadata, physicalColLoader);
    } else {
      ColumnMetadata columnMetadata = schema.add(columnSchema);
      colLoader = new NonProjectedColumn(this, columnMetadata);
    }
    assert colLoader.metadata().index() == columns.size();
    columns.add(colLoader);
    return colLoader;
  }

  @Override
  public ObjectWriter column(int colIndex) {
    ColumnLoader colLoader = columnLoader(colIndex);
    if (colLoader.isProjected()) {
      return physicalLoader.column(colLoader.vectorIndex());
    } else {
      return null;
    }
  }

  @Override
  public void setTuple(Object... values) {
    for (int i = 0; i < values.length;  i++) {
      set(i, values[i]);
    }
    resultSetLoader().saveRow();
  }

  @Override
  public void set(int colIndex, Object value) {
    ObjectWriter colWriter = column(colIndex);
    if (colWriter != null) {
      colWriter.set(value);
    }
  }

  @Override
  protected void rollOver(int overflowIndex) {
    physicalLoader.rollOver(overflowIndex);
  }

  @Override
  protected void harvest() {
    physicalLoader.harvest();
  }

  @Override
  public void buildContainer(VectorContainerBuilder containerBuilder) {
    physicalLoader.buildContainer(containerBuilder);
  }

  @Override
  public void reset() {
    physicalLoader.reset();
  }

  @Override
  public void close() {
    physicalLoader.close();
  }

  @Override
  public void reset(int index) {
    throw new IllegalStateException();
  }

  @Override
  public int lastWriteIndex() {
    throw new IllegalStateException();
  }

  @Override
  protected void resetBatch() {
    physicalLoader.resetBatch();
  }
}
