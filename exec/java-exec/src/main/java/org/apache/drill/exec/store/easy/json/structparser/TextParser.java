package org.apache.drill.exec.store.easy.json.structparser;

import com.fasterxml.jackson.core.JsonToken;

/**
 * Parses <code>true | false | null | integer | float | string |<br>
 *              embedded-object</code>
 * <p>
 * Forwards the result as a string.
 */
public class TextParser extends AbstractParser {

  private final ValueListener listener;

  public TextParser(JsonElementParser parent, String key,
      ValueListener listener) {
    super(parent, key);
    this.listener = listener;
  }

  /**
   * Parses <code>true | false | null | integer | float | string |<br>
   *              embedded-object</code>
   * <p>
   * Writes the value as text.
   */
  @Override
  public boolean parse(TokenIterator tokenizer) {
    JsonToken token = tokenizer.requireNext();
    switch (token) {
    case VALUE_NULL:
      listener.onNull();
      break;

    case VALUE_EMBEDDED_OBJECT:
    case VALUE_FALSE:
    case VALUE_TRUE:
    case VALUE_NUMBER_FLOAT:
    case VALUE_NUMBER_INT:
    case VALUE_STRING:
      listener.onString(tokenizer.textValue());
      break;

    default:
      throw errorFactory().syntaxError(token);
    }
    return true;
  }
}
