package org.apache.drill.exec.store.easy.json.loader.mongo;

import java.util.function.Consumer;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.store.easy.json.loader.AbstractValueListener;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl;
import org.apache.drill.exec.store.easy.json.loader.TupleListener;
import org.apache.drill.exec.store.easy.json.parser.ObjectListener;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.store.easy.json.parser.ObjectListener.FieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Listener for Mongo extended types. Supports both
 * <a href="https://docs.mongodb.com/manual/reference/mongodb-extended-json-v1/">
 * V1</a> and
 * <a href="https://docs.mongodb.com/manual/reference/mongodb-extended-json/">
 * V2</a> of the Mongo specs. Supports both the cannonical and relaxed modes.
 *
 * @see org.apache.drill.exec.vector.complex.fn.ExtendedTypeName ExtendedTypeName
 */
public abstract class ExtendedTypeObjectListener implements ObjectListener {
  private static final Logger logger = LoggerFactory.getLogger(ExtendedTypeObjectListener.class);

  protected final TupleListener parentTuple;
  protected final String fieldName;

  public ExtendedTypeObjectListener(TupleListener parentTuple, String fieldName) {
    this.parentTuple = parentTuple;
    this.fieldName = fieldName;
  }

  @Override
  public void bind(Consumer<ObjectListener> host) { }

  @Override
  public void onStart() { }

  @Override
  public void onEnd() { }

  protected ScalarWriter defineColumn(MinorType type) {
    return parentTuple.fieldwriterFor(
        MetadataUtils.newScalar(fieldName, type, DataMode.OPTIONAL))
        .scalar();
  }

  protected void warnIgnoredField(String typeMarker, String key) {
    logger.warn("Found unexpected field {} in Mongo extended JSON for type {}",
        key, typeMarker);
  }

  protected IllegalStateException disallowedFieldException(String key) {
    throw new IllegalStateException("Field not allowed in Mongo extended object: " + key);
  }

  public static class ExtendedTypeFieldListener extends AbstractValueListener {
    private final ExtendedTypeObjectListener objectListener;

    public ExtendedTypeFieldListener(ExtendedTypeObjectListener objectListener) {
      super (objectListener.parentTuple.loader());
      this.objectListener = objectListener;
    }

    @Override
    public void onNull() { }

    @Override
    public ObjectListener object() {
      return objectListener;
    }
  }
}
