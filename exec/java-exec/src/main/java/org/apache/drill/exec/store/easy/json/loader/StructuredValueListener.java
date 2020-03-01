package org.apache.drill.exec.store.easy.json.loader;

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.easy.json.parser.ArrayListener;
import org.apache.drill.exec.store.easy.json.parser.JsonType;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

public class StructuredValueListener extends AbstractValueListener {

  private final ColumnMetadata colSchema;

  public StructuredValueListener(JsonLoaderImpl loader, ColumnMetadata colSchema) {
    super(loader);
    this.colSchema = colSchema;
  }

  @Override
  public ColumnMetadata schema() { return colSchema; }

  @Override
  public void onNull() { }

  public static class ScalarArrayValueListener extends StructuredValueListener {

    private final ArrayListener arrayListener;

    public ScalarArrayValueListener(JsonLoaderImpl loader, ColumnMetadata colSchema, ArrayListener arrayListener) {
      super(loader, colSchema);
      this.arrayListener = arrayListener;
    }

    @Override
    public ArrayListener array(int arrayDims, JsonType type) {
      Preconditions.checkArgument(arrayDims == 1);
      return arrayListener;
    }
  }
}
