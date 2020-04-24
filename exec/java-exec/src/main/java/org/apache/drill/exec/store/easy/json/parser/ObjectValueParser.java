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
    if (token == JsonToken.START_OBJECT) {
      // Position: { ^
      objectParser.parse(tokenizer);
    } else {
      throw errorFactory().structureError("JSON object expected");
    }
  }
}
