package org.apache.drill.exec.store.easy.json.loader;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.easy.json.parser.ArrayListener;
import org.apache.drill.exec.store.easy.json.parser.ObjectListener;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;

/**
 * Abstract base class for value (field or element) listeners.
 */
public abstract class AbstractValueListener implements ValueListener {

  protected final JsonLoaderImpl loader;

  public AbstractValueListener(JsonLoaderImpl loader) {
    this.loader = loader;
  }

  @Override
  public void bind(ValueHost host) { }

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
  public ArrayListener array(ValueDef valueDef) {
    throw loader.typeConversionError(schema(), valueDef);
  }

  protected UserException typeConversionError(String jsonType) {
    return loader.typeConversionError(schema(), jsonType);
  }

  protected abstract ColumnMetadata schema();
}
