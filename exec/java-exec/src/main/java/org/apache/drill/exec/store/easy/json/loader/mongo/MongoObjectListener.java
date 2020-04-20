package org.apache.drill.exec.store.easy.json.loader.mongo;

import java.util.function.Consumer;

import org.apache.drill.exec.store.easy.json.parser.ObjectListener;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;

public class MongoObjectListener implements ObjectListener {

  @Override
  public void bind(Consumer<ObjectListener> host) { }

  @Override
  public void onStart() { }

  @Override
  public FieldType fieldType(String key) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ValueListener addField(String key, ValueDef valueDef) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void onEnd() {
    // TODO Auto-generated method stub

  }

}
