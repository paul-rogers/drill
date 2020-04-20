package org.apache.drill.exec.store.easy.json.loader.mongo;

import org.apache.drill.exec.store.easy.json.loader.TupleListener;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.exec.vector.complex.fn.ExtendedTypeName;

public class DateObjectListener extends ExtendedTypeObjectListener {

  public DateObjectListener(TupleListener parentTuple, String fieldName) {
    super(parentTuple, fieldName);
    // TODO Auto-generated constructor stub
  }

  private static final String TYPE_MARKER = "$date";

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
    // TODO Auto-generated method stub
    return null;
  }

}
