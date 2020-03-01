package org.apache.drill.exec.store.easy.json.loader;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.easy.json.parser.ArrayListener;
import org.apache.drill.exec.store.easy.json.parser.JsonType;
import org.apache.drill.exec.store.easy.json.parser.ObjectListener;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;

public abstract class AbstractValueListener implements ValueListener {

  protected final JsonLoaderImpl loader;

  public AbstractValueListener(JsonLoaderImpl loader) {
    this.loader = loader;
  }

  @Override
  public boolean isText() { return false; }

  @Override
  public void onBoolean(boolean value) {
    throw typeConversionError("Boolean");
  }

  @Override
  public void onInt(long value) {
    throw typeConversionError("integer");
  }

  @Override
  public void onFloat(double value) {
    throw typeConversionError("float");
  }

  @Override
  public void onString(String value) {
    throw typeConversionError("string");
  }

  @Override
  public void onEmbedddObject(String value) {
    throw typeConversionError("object");
  }

  @Override
  public ObjectListener object() {
    throw typeConversionError("object");
  }

  @Override
  public ArrayListener array(int arrayDims, JsonType type) {
    throw typeConversionError(type.name() + " array[" + arrayDims + "]");
  }

  @Override
  public ArrayListener objectArray(int arrayDims) {
    throw typeConversionError("object array[" + arrayDims + "]");
  }

  protected UserException typeConversionError(String jsonType) {
    return loader.typeConversionError(schema(), jsonType);
  }

  protected abstract ColumnMetadata schema();

}
