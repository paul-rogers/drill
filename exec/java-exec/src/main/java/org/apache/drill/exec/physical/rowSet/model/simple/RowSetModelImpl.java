package org.apache.drill.exec.physical.rowSet.model.simple;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.rowSet.model.TupleModel;
import org.apache.drill.exec.physical.rowSet.model.TupleModel.RowSetModel;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractMapVector;

public class RowSetModelImpl extends SimpleTupleModelImpl implements RowSetModel {

  public static class PrimitiveColumnModel extends SimpleColumnModelImpl {
    protected final ValueVector vector;

    public PrimitiveColumnModel(ColumnMetadata schema, ValueVector vector) {
      super(schema);
      this.vector = vector;
    }

    @Override
    public <R, A> R visit(ModelVisitor<R, A> visitor, A arg) {
      if (schema.isArray()) {
        return visitor.visitPrimitiveArrayColumn(this, arg);
      } else {
        return visitor.visitPrimitiveColumn(this, arg);
      }
    }

    public ValueVector vector() { return vector; }
  }

  public static class MapColumnModel extends SimpleColumnModelImpl {
    private final MapModel mapModel;
    private final AbstractMapVector vector;

    public MapColumnModel(ColumnMetadata colSchema, AbstractMapVector vector, MapModel MapModel) {
      super(colSchema);
      this.vector = vector;
      this.mapModel = MapModel;
    }

    @Override
    public <R, A> R visit(ModelVisitor<R, A> visitor, A arg) {
      if (schema.isArray()) {
        return visitor.visitMapArrayColumn(this, arg);
      } else {
        return visitor.visitMapColumn(this, arg);
      }
    }

    @Override
    public TupleModel mapModel() { return mapModel; }

    public MapModel mapModelImpl() { return mapModel; }

    public AbstractMapVector vector() { return vector; }
  }

  public static class MapModel extends SimpleTupleModelImpl {
    private final AbstractMapVector vector;

    public MapModel(AbstractMapVector vector) {
      this.vector = vector;
    }

    @Override
    public <R, A> R visit(ModelVisitor<R, A> visitor, A arg) {
      if (schema.parent().isArray()) {
        return visitor.visitMapArray(this, arg);
      } else {
        return visitor.visitMap(this, arg);
      }
    }

    public AbstractMapVector vector() { return vector; }

    @Override
    public BufferAllocator allocator() {
      return vector.getAllocator();
    }
  }

  private final VectorContainer container;

  public RowSetModelImpl(VectorContainer container) {
    this.container = container;
  }

  public static RowSetModelImpl fromContainer(VectorContainer container) {
    return new ModelBuilder().buildModel(container);
  }

  public static RowSetModelImpl fromSchema(BufferAllocator allocator,
      TupleMetadata schema) {
    return new ModelStructureBuilder(allocator).buildModel(schema);
  }

  @Override
  public BufferAllocator allocator() { return container.getAllocator(); }

  @Override
  public VectorContainer container() { return container; }

  @Override
  public <R, A> R visit(ModelVisitor<R, A> visitor, A arg) {
    return visitor.visitRow(this, arg);
  }
}
