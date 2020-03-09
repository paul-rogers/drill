package org.apache.drill.exec.store.easy.json.loader;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ObjectValueListener;
import org.apache.drill.exec.store.easy.json.parser.ArrayListener;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.exec.vector.accessor.ArrayWriter;

/**
 * Base class for scalar and object arrays.
 */
public abstract class AbstractArrayListener implements ArrayListener {

  protected final JsonLoaderImpl loader;
  protected final ColumnMetadata colSchema;
  protected final ValueListener elementListener;

  public AbstractArrayListener(JsonLoaderImpl loader, ColumnMetadata colSchema, ValueListener elementListener) {
    this.loader = loader;
    this.colSchema = colSchema;
    this.elementListener = elementListener;
  }

  public ValueListener elementListener() { return elementListener; }

  @Override
  public void onStart() { }

  @Override
  public void onElementStart() { }

  @Override
  public void onElementEnd() { }

  @Override
  public void onEnd() { }

  @Override
  public ValueListener element(ValueDef valueDef) {
    throw loader.typeConversionError(colSchema, valueDef);
  }

  protected UserException typeConversionError(String jsonType) {
    return loader.typeConversionError(colSchema, jsonType);
  }

  public static class ScalarArrayListener extends AbstractArrayListener {

    public ScalarArrayListener(JsonLoaderImpl loader, ColumnMetadata colSchema, ScalarListener valueListener) {
      super(loader, colSchema, valueListener);
    }

    @Override
    public ValueListener element(ValueDef valueDef) {
      return elementListener;
    }
  }

  public static class ObjectArrayListener extends AbstractArrayListener {
    private final ArrayWriter arrayWriter;

    public ObjectArrayListener(JsonLoaderImpl loader, ArrayWriter arrayWriter, ObjectValueListener valueListener) {
      super(loader, arrayWriter.schema(), valueListener);
      this.arrayWriter = arrayWriter;
    }

    @Override
    public ValueListener element(ValueDef valueDef) {
      return elementListener;
    }

    @Override
    public void onElementEnd() {
      arrayWriter.save();
    }
  }
}
