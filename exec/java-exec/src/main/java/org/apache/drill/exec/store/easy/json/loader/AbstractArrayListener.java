package org.apache.drill.exec.store.easy.json.loader;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ObjectValueListener;
import org.apache.drill.exec.store.easy.json.parser.ArrayListener;
import org.apache.drill.exec.store.easy.json.parser.JsonType;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
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
  public void onElementStart() { }

  @Override
  public void onElementEnd() { }

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

    private final ScalarListener valueListener;

    public ScalarArrayListener(JsonLoaderImpl loader, ColumnMetadata colSchema, ScalarListener valueListener) {
      super(loader, colSchema);
      this.valueListener = valueListener;
    }

    @Override
    public ValueListener scalarElement(JsonType type) {
      return valueListener;
    }

    public ScalarListener elementListener() { return valueListener; }
  }

  public static class ObjectArrayListener extends AbstractArrayListener {
    private final ArrayWriter arrayWriter;
    private final ObjectValueListener valueListener;

    public ObjectArrayListener(JsonLoaderImpl loader, ArrayWriter arrayWriter, ObjectValueListener valueListener) {
      super(loader, arrayWriter.schema());
      this.arrayWriter = arrayWriter;
      this.valueListener = valueListener;
    }

    @Override
    public ValueListener objectElement() {
      return valueListener;
    }

    // Called with a provided schema where the initial array
    // value is empty.
    @Override
    public ValueListener scalarElement(JsonType type) {
      Preconditions.checkArgument(type == JsonType.EMPTY || type == JsonType.NULL);
      return valueListener;
    }

    @Override
    public void onElementEnd() {
      arrayWriter.save();
    }
  }
}
