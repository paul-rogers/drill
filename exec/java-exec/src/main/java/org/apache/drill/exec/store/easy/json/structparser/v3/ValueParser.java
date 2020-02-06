package org.apache.drill.exec.store.easy.json.structparser.v3;

import com.fasterxml.jackson.core.JsonToken;

public class ValueParser extends AbstractElementParser {

  private interface ValueHandler {
    void accept(TokenIterator tokenizer, JsonToken token);
  }

  /**
   * Parses <code>true | false | null | integer | float | string |<br>
   *              embedded-object</code>
   * <p>
   * Forwards the result as a typed value.
   */
  public class TypedValueHandler implements ValueHandler {

    @Override
    public void accept(TokenIterator tokenizer, JsonToken token) {
      switch (token) {
        case VALUE_NULL:
          listener.onNull();
          break;
        case VALUE_TRUE:
          listener.onBoolean(true);
          break;
        case VALUE_FALSE:
          listener.onBoolean(false);
          break;
        case VALUE_NUMBER_INT:
          listener.onInt(tokenizer.longValue());
          break;
        case VALUE_NUMBER_FLOAT:
          listener.onFloat(tokenizer.doubleValue());
          break;
        case VALUE_STRING:
          listener.onString(tokenizer.stringValue());
          break;
        case VALUE_EMBEDDED_OBJECT:
          listener.onEmbedddObject(tokenizer.stringValue());
        default:
          throw errorFactory().structureError(key(), token);
      }
    }
  }

  /**
   * Parses <code>true | false | null | integer | float | string |<br>
   *              embedded-object</code>
   * <p>
   * Forwards the result as a string.
   */
  public class TextValueHandler implements ValueHandler {

    @Override
    public void accept(TokenIterator tokenizer, JsonToken token) {
      switch (token) {
        case VALUE_EMBEDDED_OBJECT:
        case VALUE_FALSE:
        case VALUE_TRUE:
        case VALUE_NUMBER_FLOAT:
        case VALUE_NUMBER_INT:
        case VALUE_STRING:
          listener.onString(tokenizer.textValue());
          break;

        default:
          throw errorFactory().structureError(key(), token);
      }
    }
  }

  private final String key;
  private final ValueListener listener;
  private final ValueHandler valueHandler;
  private ObjectParser objectParser;
  private ArrayParser arrayParser;

  public ValueParser(ElementParser parent, String key, ValueListener listener) {
    super(parent);
    this.key = key;
    this.listener = listener;
    if (listener.isText() || structParser().options().allTextMode) {
      valueHandler = new TextValueHandler();
    } else {
      valueHandler = new TypedValueHandler();
    }
  }

  public String key() { return key; }

  public ValueListener listener() { return listener; }

  public void bindObjectParser(ObjectParser parser) {
    objectParser = parser;
  }

  public void bindArrayParser(ArrayParser parser) {
    arrayParser = parser;
  }

  /**
   * Parses <code>true | false | null | integer | float | string|
   *              embedded-object | { ... } | [ ... ]</code>
   */
  @Override
  public void parse(TokenIterator tokenizer) {
    JsonToken token = tokenizer.requireNext();
    switch (token) {
    case START_OBJECT:
      // Position: { ^
      if (objectParser == null) {
        // No object parser yet. May be that the value was null,
        // or may be that it changed types.
        objectParser = ValueFactory.objectParser(this);
      }
      objectParser.parse(tokenizer);
      break;

    case START_ARRAY:
      // Position: { ^
      if (arrayParser == null) {
        // No array parser yet. May be that the value was null,
        // or may be that it changed types.
        arrayParser = ValueFactory.createArrayParser(this, tokenizer);
      }
      arrayParser.parse(tokenizer);
      break;

    case VALUE_NULL:
      listener.onNull();
      break;

    default:
      valueHandler.accept(tokenizer, token);
    }
  }
}
