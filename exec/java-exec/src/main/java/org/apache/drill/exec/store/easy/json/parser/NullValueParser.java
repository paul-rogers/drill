package org.apache.drill.exec.store.easy.json.parser;

import org.apache.drill.exec.store.easy.json.parser.ObjectParser.FieldDefn;

import com.fasterxml.jackson.core.JsonToken;

/**
 * Parses nulls. On the first non-null token, replaces itself with
 * a "resolved" parser to handle the actual structure.
 */
public class NullValueParser extends AbstractElementParser {

  private final ObjectParser parent;
  protected final String key;

  public NullValueParser(ObjectParser parent, String key) {
    super(parent.structParser());
    this.parent = parent;
    this.key = key;
  }

  /**
   * Parses nulls. On the first non-null
   * Parses <code>true | false | null | integer | float | string|
   *              embedded-object | { ... } | [ ... ]</code>
   */
  @Override
  public void parse(TokenIterator tokenizer) {
    JsonToken token = tokenizer.requireNext();
    if (token != JsonToken.VALUE_NULL) {
      tokenizer.unget(token);
      resolve(tokenizer).parse(tokenizer);

      // This parser never called again
    }
  }

  /**
   * Replace this parser with a new parser based on the current
   * parse context.
   */
  protected ElementParser resolve(TokenIterator tokenizer) {
    ElementParser fieldParser = parent.onField(new FieldDefn(parent, tokenizer, key));
    parent.replaceFieldParser(key, fieldParser);
    return fieldParser;
  }
}
