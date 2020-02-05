package org.apache.drill.exec.store.easy.json.structparser;

public class ElementParserFactory {

  private final JsonElementParser parent;
  private final String key;

  public ElementParserFactory(JsonElementParser parent, String key) {
    this.parent = parent;
    this.key = key;
  }

  public JsonElementParser textParser(ValueListener listener) {
    return new TextParser(parent, key, listener);
  }

  public JsonElementParser valueParser(ValueListener listener) {
    return new ValueParser(parent, key, listener);
  }

  public JsonElementParser dummyParser() {
    new DummyValueParser(parent, key);
  }
}
