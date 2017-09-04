package org.apache.drill.exec.physical.rowSet.model.single;

import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.rowSet.ResultVectorCache;
import org.apache.drill.exec.physical.rowSet.impl.NullResultVectorCacheImpl;
import org.apache.drill.exec.record.TupleSchema;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.record.TupleSchema.AbstractColumnMetadata;
import org.apache.drill.exec.record.TupleSchema.MapColumnMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractMapVector;

public class BaseStructureBuilder implements StructureBuilder {

  private final ResultVectorCache vectorCache;

  public BaseStructureBuilder(BufferAllocator allocator) {
    this.vectorCache = new NullResultVectorCacheImpl(allocator);
  }

  public BaseStructureBuilder(ResultVectorCache vectorCache) {
    this.vectorCache = vectorCache;
  }

  @Override
  public BufferAllocator allocator() { return vectorCache.allocator(); }

  @Override
  public AbstractSingleColumnModel addColumn(AbstractSingleTupleModel tupleModel, ColumnMetadata columnSchema) {
    if (columnSchema.isMap()) {
      return buildMap(tupleModel, columnSchema);
    } else {
      return buildPrimitive(tupleModel, columnSchema);
    }
  }

  @SuppressWarnings("resource")
  private PrimitiveColumnModel buildPrimitive(AbstractSingleTupleModel tupleModel, ColumnMetadata columnSchema) {

    // Create the vector for the column.

    ValueVector vector = buildPrimitiveVector(columnSchema);

    // Create the column writer and add it.

    PrimitiveColumnModel colModel = new PrimitiveColumnModel(columnSchema, vector);
    tupleModel.addColumnImpl(colModel);

    preparePrimitiveColumn(tupleModel, colModel);
    return colModel;
  }

  ValueVector buildPrimitiveVector(ColumnMetadata columnSchema) {
    return vectorCache.addOrGet(columnSchema.schema());
  }

  protected void preparePrimitiveColumn(AbstractSingleTupleModel tupleModel,
      PrimitiveColumnModel colModel) { }

  @SuppressWarnings("resource")
  private AbstractSingleColumnModel buildMap(
      AbstractSingleTupleModel tupleModel, ColumnMetadata columnSchema) {

    // When dynamically adding columns, must add the (empty)
    // map by itself, then add columns to the map via separate
    // calls.

    assert columnSchema.isMap();
    assert columnSchema.mapSchema().size() == 0;

    AbstractMapVector mapVector = buildMapVector(columnSchema);

    if (mapVector != null) {

      // Creating the vector cloned the schema. Replace the
      // field in the column metadata to match the one in the vector.
      // Doing so is an implementation hack, so access a method on the
      // implementation class.

      ((AbstractColumnMetadata) columnSchema).replaceField(mapVector.getField());
    }

    // Build the map model from a matching triple of schema, container and
    // column models.

    MapModel mapModel = new MapModel((TupleSchema) columnSchema.mapSchema(), mapVector);

    // Create the map model with all the pieces.

    MapColumnModel mapColModel = new MapColumnModel((MapColumnMetadata) columnSchema, mapVector, mapModel);
    tupleModel.addColumnImpl(mapColModel);

    prepareMapColumn(tupleModel, mapColModel);
    return mapColModel;
  }

  protected void prepareMapColumn(AbstractSingleTupleModel tupleModel,
      MapColumnModel mapColModel) { }

  protected AbstractMapVector buildMapVector(ColumnMetadata columnSchema) {

    // Don't get the map vector from the vector cache. Map vectors may
    // have content that varies from batch to batch. Only the leaf
    // vectors can be cached.

    return (AbstractMapVector) TypeHelper.getNewVector(
        columnSchema.schema(),
        allocator(),
        null);
  }
}