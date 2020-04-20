package org.apache.drill.exec.store.easy.json.loader.mongo;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl;
import org.apache.drill.exec.store.easy.json.loader.ScalarListener;
import org.apache.drill.exec.store.easy.json.loader.TupleListener;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.exec.store.easy.json.parser.ObjectListener.FieldType;
import org.apache.drill.exec.vector.accessor.ScalarWriter;

/**
 * Mongo extended Int64 handler.
 *
 * @see <a href="https://docs.mongodb.com/manual/reference/mongodb-extended-json/#bson.Int64>Int64</a>
 */
public class Int64ObjectListener extends ExtendedTypeObjectListener {
  public static final String TYPE_MARKER = ExtendedTypeNames.LONG;

  public Int64ObjectListener(TupleListener parentTuple, String fieldName) {
    super(parentTuple, fieldName);
  }

  @Override
  public FieldType fieldType(String key) {
    switch (key) {
    case TYPE_MARKER:
      return FieldType.TYPED;
    default:
      warnIgnoredField(TYPE_MARKER, key);
      return FieldType.IGNORE;
    }
  }

  @Override
  public ValueListener addField(String key, ValueDef valueDef) {
    switch (key) {
    case TYPE_MARKER:
      return new Int64Listener(parentTuple.loader(),
          defineColumn(MinorType.BIGINT));
    default:
      throw disallowedFieldException(key);
    }
  }

  public class Int64Listener extends ScalarListener {

    public Int64Listener(JsonLoaderImpl loader, ScalarWriter writer) {
      super(loader, writer);
    }

    @Override
    public void onInt(long value) {
      writer.setLong(value);
    }

    @Override
    public void onString(String value) {
      value = value.trim();
      if (value.isEmpty()) {
        setNull();
      } else {
        try {
          writer.setLong(Long.parseLong(value));
        } catch (NumberFormatException e) {
          throw loader.dataConversionError(schema(), "string", value);
        }
      }
    }

    @Override
    protected void setArrayNull() {
      writer.setLong(0);
    }
  }
}
