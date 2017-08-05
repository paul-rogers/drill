package org.apache.drill.exec.physical.rowSet.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.record.TupleSchema;
import org.apache.drill.exec.record.TupleSchema.AbstractColumnMetadata;

public abstract class BaseTupleModel implements TupleModel {

  public static abstract class BaseColumnModel implements ColumnModel {
    protected final ColumnMetadata schema;

    public BaseColumnModel(ColumnMetadata schema) {
      this.schema = schema;
    }

    @Override
    public ColumnMetadata schema() { return schema; }

    @Override
    public TupleModel mapModel() { return null; }
  }

  protected final List<ColumnModel> columns = new ArrayList<>();
  protected final TupleSchema schema;

  public BaseTupleModel() {
    this.schema = new TupleSchema();
  }

  @Override
  public TupleMetadata schema() { return schema; }

  @Override
  public int size() { return schema.size(); }

  @Override
  public ColumnModel column(int index) {
    return columns.get(index);
  }

  @Override
  public ColumnModel column(String name) {
    return column(schema.index(name));
  }

  public void add(ColumnModel column) {
    schema.add((AbstractColumnMetadata) column.schema());
    columns.add(column);
  }
}
