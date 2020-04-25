package org.apache.drill.exec.store.easy.json.parser;

import com.fasterxml.jackson.core.JsonToken;

public class ObjectValueParser extends AbstractElementParser {

  private final ObjectParser objectParser;

  public ObjectValueParser(ObjectParser objectParser) {
    super(objectParser.structParser());
    this.objectParser = objectParser;
  }

  /**
   * Parses <code>{ ... }</code>
   */
  @Override
  public void parse(TokenIterator tokenizer) {
    JsonToken token = tokenizer.requireNext();
    switch (token) {
      case START_OBJECT:
        objectParser.parse(tokenizer);
        break;
      case VALUE_NULL:
        // Silently ignore, treat as a missing field
        break;
      default:
        throw errorFactory().structureError("JSON object expected");
    }
  }
}
