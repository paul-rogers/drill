package org.apache.drill.exec.physical.rowSet.model.simple;

import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.rowSet.model.simple.RowSetModelImpl.MapColumnModel;
import org.apache.drill.exec.physical.rowSet.model.simple.RowSetModelImpl.MapModel;
import org.apache.drill.exec.physical.rowSet.model.simple.RowSetModelImpl.PrimitiveColumnModel;
import org.apache.drill.exec.physical.rowSet.model.simple.SimpleTupleModelImpl.SimpleColumnModelImpl;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractMapVector;

public class ModelStructureBuilder {

  private BufferAllocator allocator;

  public ModelStructureBuilder(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  public RowSetModelImpl buildModel(TupleMetadata schema) {
    VectorContainer container = new VectorContainer(allocator);
    RowSetModelImpl rowModel = new RowSetModelImpl(container);
    rowModel.addAll(schema, this);
    return rowModel;
  }

  @SuppressWarnings("resource")
  public SimpleColumnModelImpl buildColumn(ColumnMetadata schema) {
    ValueVector vector = TypeHelper.getNewVector(schema.schema(), allocator, null);
    if (schema.isMap()) {
      AbstractMapVector mapVector = (AbstractMapVector) vector;
      MapModel mapModel = new MapModel(mapVector);
      mapModel.addAll(schema.mapSchema(), this);
      MapColumnModel map = new MapColumnModel(schema, mapVector, mapModel);
      return map;
    } else {
      return new PrimitiveColumnModel(schema, vector);
    }
  }
}
