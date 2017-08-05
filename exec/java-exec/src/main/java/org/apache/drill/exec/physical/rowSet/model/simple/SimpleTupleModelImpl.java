package org.apache.drill.exec.physical.rowSet.model.simple;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.rowSet.model.BaseTupleModel;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.record.TupleSchema;

public abstract class SimpleTupleModelImpl extends BaseTupleModel {

  public static abstract class SimpleColumnModelImpl extends BaseColumnModel {

    public SimpleColumnModelImpl(ColumnMetadata schema) {
      super(schema);
    }

    public abstract <R, A> R visit(ModelVisitor<R, A> visitor, A arg);
  }

  @Override
  public ColumnModel add(MaterializedField field) {
    return add(TupleSchema.fromField(field));
  }

  @Override
  public ColumnModel add(ColumnMetadata colMetadata) {
    ModelStructureBuilder builder = new ModelStructureBuilder(allocator());
    SimpleColumnModelImpl colModel = builder.buildColumn(colMetadata);
    columns.add(colModel);
    return colModel;
  }

  public abstract BufferAllocator allocator();

  protected void addAll(TupleMetadata newSchema, ModelStructureBuilder builder) {
    for (int i = 0; i < newSchema.size(); i++) {
      columns.add(builder.buildColumn(newSchema.metadata(i)));
    }
  }

  public abstract <R, A> R  visit(ModelVisitor<R, A> visitor, A arg);

  public <R, A> R visitChildren(ModelVisitor<R, A> visitor, A arg) {
    for (ColumnModel colModel : columns) {
      ((SimpleColumnModelImpl) colModel).visit(visitor, arg);
    }
    return null;
  }
}