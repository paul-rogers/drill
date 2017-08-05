package org.apache.drill.exec.physical.rowSet.model.simple;

import org.apache.drill.exec.physical.rowSet.model.BaseTupleModel.BaseColumnModel;
import org.apache.drill.exec.physical.rowSet.model.simple.RowSetModelImpl.MapColumnModel;
import org.apache.drill.exec.physical.rowSet.model.simple.RowSetModelImpl.MapModel;
import org.apache.drill.exec.physical.rowSet.model.simple.RowSetModelImpl.PrimitiveColumnModel;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.record.TupleMetadata.StructureType;
import org.apache.drill.exec.record.TupleSchema;
import org.apache.drill.exec.record.TupleSchema.AbstractColumnMetadata;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractMapVector;

public class ModelBuilder {

  public RowSetModelImpl buildModel(VectorContainer container) {
    RowSetModelImpl rowModel = new RowSetModelImpl(container);
    for (int i = 0; i < container.getNumberOfColumns(); i++) {
      @SuppressWarnings("resource")
      ValueVector vector = container.getValueVector(i).getValueVector();
      rowModel.add(buildColumn(vector));
    }
    return rowModel;
  }

  private BaseColumnModel buildColumn(ValueVector vector) {
    ColumnMetadata colSchema = TupleSchema.fromField(vector.getField());
    if (colSchema.structureType() == StructureType.TUPLE) {
      return buildMapColumn(colSchema, (AbstractMapVector) vector);
    } else {
      return new PrimitiveColumnModel(colSchema, vector);
    }
  }

  private BaseColumnModel buildMapColumn(ColumnMetadata schema, AbstractMapVector vector) {
    AbstractColumnMetadata colSchema = TupleSchema.fromField(vector.getField());
    MapModel mapModel = buildMap(vector);
    return new MapColumnModel(colSchema, vector, mapModel);
  }

  private MapModel buildMap(AbstractMapVector vector) {
    MapModel mapModel = new MapModel(vector);
    for (ValueVector child : vector) {
      mapModel.add(buildColumn(child));
    }
    return mapModel;
  }
}
