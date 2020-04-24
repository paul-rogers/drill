package org.apache.drill.exec.store.easy.json.parser;

import org.apache.drill.exec.store.easy.json.parser.ObjectParser.FieldDefn;

import com.fasterxml.jackson.core.JsonToken;

/**
 * Represents an empty array: the case where the parser has seen only
 * {@code []}, but no array elements which would indicate the type.
 * Resolves to a specific type upon
 * presentation of the first element. If that element is
 * {@code null}, we must still choose a type to record nulls.
 * <p>
 * This array listener holds no element since none has been
 * created yet; we use this only while we see empty arrays.
 */
public class EmptyArrayParser extends AbstractElementParser {

  private final ObjectParser parent;
  protected final String key;

  public EmptyArrayParser(ObjectParser parent, String key) {
    super(parent.structParser());
    this.parent = parent;
    this.key = key;
  }

  @Override
  public void parse(TokenIterator tokenizer) {
    JsonToken token1 = tokenizer.requireNext();

    // Ignore null: treat as an empty array.
    if (token1 == JsonToken.VALUE_NULL) {
      return;
    }

    // Must be an array
    if (token1 != JsonToken.START_ARRAY) {
      throw errorFactory().structureError("Encountered an object where an array is expected");
    }

    // Ignore an empty array, resolve a non-empty array.
    // Must be done even if the array value is null so elements
    // can be counted.
    JsonToken token2 = tokenizer.requireNext();
    if (token2 != JsonToken.END_ARRAY) {

      // Saw the first actual element. Swap out this parser for a
      // real field parser, then let that parser parse from here.
      tokenizer.unget(token2);
      tokenizer.unget(token1);
      resolve(token2, tokenizer).parse(tokenizer);

      // This parser never called again
    }
  }

  /**
   * Replace this parser with a new parser based on the current
   * parse context.
   */
  protected ElementParser resolve(JsonToken token, TokenIterator tokenizer) {
    ElementParser fieldParser;
    if (token == JsonToken.VALUE_NULL) {
      fieldParser = resolveNullElement();
    } else {
      fieldParser = parent.onField(new FieldDefn(parent, tokenizer, key));
    }
    parent.replaceFieldParser(key, fieldParser);
    return fieldParser;
  }

  /**
   * Create a parser when the first element seen is a null. There is no "natural"
   * parser for this case, the parser must be selected arbitrarily.
    */
  protected ElementParser resolveNullElement() {
    throw errorFactory().structureError("Don't know how to resolve [ null ]");
  }
}
