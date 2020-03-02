package org.apache.drill.exec.store.easy.json.loader;

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.easy.json.loader.AbstractArrayListener.ScalarArrayListener;
import org.apache.drill.exec.store.easy.json.loader.TupleListener.MapListener;
import org.apache.drill.exec.store.easy.json.parser.ArrayListener;
import org.apache.drill.exec.store.easy.json.parser.JsonType;
import org.apache.drill.exec.store.easy.json.parser.ObjectListener;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

public class StructuredValueListener extends AbstractValueListener {

  private final ColumnMetadata colSchema;

  public StructuredValueListener(JsonLoaderImpl loader, ColumnMetadata colSchema) {
    super(loader);
    this.colSchema = colSchema;
  }

  @Override
  public ColumnMetadata schema() { return colSchema; }

  // Ignore array nulls: {a: null} is the same as omitting
  // array column a: an array of zero elements
  @Override
  public void onNull() { }

  /**
   * Value listener for a scalar array (Drill repeated primitive).
   * Maps null values for the entire array to an empty array.
   * Maps a scalar to an array with a single value.
   */
  public static class ScalarArrayValueListener extends StructuredValueListener {

    private final ArrayListener arrayListener;
    private final ScalarListener elementListener;

    public ScalarArrayValueListener(JsonLoaderImpl loader, ColumnMetadata colSchema, ScalarArrayListener arrayListener) {
      super(loader, colSchema);
      this.arrayListener = arrayListener;
      this.elementListener = arrayListener.elementListener();
    }

    @Override
    public ArrayListener array(int arrayDims, JsonType type) {
      Preconditions.checkArgument(arrayDims == 1);
      return arrayListener;
    }

    public static ValueListener listenerFor(JsonLoaderImpl loader,
        TupleWriter tupleWriter, ColumnMetadata colSchema) {
      return new ScalarArrayValueListener(loader, colSchema,
          new ScalarArrayListener(loader, colSchema,
              ScalarListener.listenerFor(loader, tupleWriter, colSchema)));
    }

    @Override
    public void onBoolean(boolean value) {
      elementListener.onBoolean(value);
    }

    @Override
    public void onInt(long value) {
      elementListener.onInt(value);
    }

    @Override
    public void onFloat(double value) {
      elementListener.onFloat(value);
    }

    @Override
    public void onString(String value) {
      elementListener.onString(value);
    }
  }

  public static class ObjectValueListener extends StructuredValueListener {

    private final ObjectListener tupleListener;

    public ObjectValueListener(JsonLoaderImpl loader, ColumnMetadata colSchema, ObjectListener tupleListener) {
      super(loader, colSchema);
      this.tupleListener = tupleListener;
    }

    public static ObjectValueListener listenerFor(JsonLoaderImpl loader,
        TupleWriter tupleWriter, String key, TupleMetadata providedSchema) {
      ColumnMetadata colSchema = MetadataUtils.newMap(key);
      int index = tupleWriter.addColumn(colSchema);
      return new ObjectValueListener(loader, colSchema,
          new MapListener(loader, tupleWriter.tuple(index), providedSchema));
    }

    @Override
    public ObjectListener object() {
      return tupleListener;
    }
  }
}
