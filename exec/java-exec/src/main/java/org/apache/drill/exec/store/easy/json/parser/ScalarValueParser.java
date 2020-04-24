package org.apache.drill.exec.store.easy.json.parser;

import com.fasterxml.jackson.core.JsonToken;

public abstract class ScalarValueParser extends ValueParser {

  public ScalarValueParser(JsonStructureParser structParser, ValueListener listener) {
    super(structParser, listener);
  }

  /**
   * Parses <code>true | false | null | integer | float | string|
   *              embedded-object</code>
   */
  @Override
  public void parse(TokenIterator tokenizer) {
    JsonToken token = tokenizer.requireNext();
    if (token.isScalarValue()) {
      parseValue(tokenizer, token);
    } else {
      throw errorFactory().structureError("Structure value found where scalar expected");
    }
  }

  protected abstract void parseValue(TokenIterator tokenizer, JsonToken token);

  /**
   * Parses <code>true | false | null | integer | float | string |<br>
   *              embedded-object</code><br>
   * and simply passes the value token on to the listener.
   */
  public static class SimpleValueParser extends ScalarValueParser {

    public SimpleValueParser(JsonStructureParser structParser, ValueListener listener) {
      super(structParser, listener);
    }

    @Override
    public void parseValue(TokenIterator tokenizer, JsonToken token) {
      listener.onValue(token, tokenizer);
    }
  }

  /**
   * Parses <code>true | false | null | integer | float | string |<br>
   *              embedded-object</code>
   * <p>
   * Forwards the result as a string.
   */
  public static class TextValueParser extends ScalarValueParser {

    public TextValueParser(JsonStructureParser structParser, ValueListener listener) {
      super(structParser, listener);
    }

    @Override
    public void parseValue(TokenIterator tokenizer, JsonToken token) {
      if (token == JsonToken.VALUE_NULL) {
        listener.onValue(token, tokenizer);
      } else {
        listener.onText(
            token == JsonToken.VALUE_NULL ? null : tokenizer.textValue());
      }
    }
  }
}
