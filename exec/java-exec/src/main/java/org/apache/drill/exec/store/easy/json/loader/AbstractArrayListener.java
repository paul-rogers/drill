package org.apache.drill.exec.store.easy.json.loader;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.easy.json.parser.ArrayListener;
import org.apache.drill.exec.store.easy.json.parser.JsonType;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

public class AbstractArrayListener implements ArrayListener {

  protected final JsonLoaderImpl loader;
  private final ColumnMetadata colSchema;

  public AbstractArrayListener(JsonLoaderImpl loader, ColumnMetadata colSchema) {
    this.loader = loader;
    this.colSchema = colSchema;
  }

  @Override
  public void onStart() { }

  @Override
  public void onElement() { }

  @Override
  public void onEnd() { }

  @Override
  public ValueListener scalarElement(JsonType type) {
    throw typeConversionError("Boolean");
  }

  @Override
  public ValueListener arrayElement(int arrayDims, JsonType type) {
    throw typeConversionError(type.name() + " array[" + arrayDims + "]");
  }

  @Override
  public ValueListener objectElement() {
    throw typeConversionError("object");
  }

  @Override
  public ValueListener objectArrayElement(int arrayDims) {
    throw typeConversionError("object array[" + arrayDims + "]");
  }

  protected UserException typeConversionError(String jsonType) {
    return loader.typeConversionError(colSchema, jsonType);
  }

  public static class ScalarArrayListener extends AbstractArrayListener {

    private final ValueListener valueListener;

    public ScalarArrayListener(JsonLoaderImpl loader, ColumnMetadata colSchema, ValueListener valueListener) {
      super(loader, colSchema);
      this.valueListener = valueListener;
    }

    @Override
    public ValueListener arrayElement(int arrayDims, JsonType type) {
      Preconditions.checkArgument(arrayDims == 0);
      return valueListener;
    }
  }
}
