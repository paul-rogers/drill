package org.apache.drill.exec.store.easy.json.parser;

import com.fasterxml.jackson.core.JsonToken;

public class ArrayValueParser extends AbstractElementParser {

  protected ArrayParser arrayParser;

  public ArrayValueParser(ArrayParser arrayParser) {
    super(arrayParser.structParser());
    this.arrayParser = arrayParser;
  }

  /**
   * Parses <code>true | false | null | integer | float | string|
   *              embedded-object | [ ... ]</code>
   */
  @Override
  public void parse(TokenIterator tokenizer) {
    JsonToken token = tokenizer.requireNext();
    if (token == JsonToken.START_ARRAY) {
      // Position: [ ^
      arrayParser.parse(tokenizer);
    } else if (token.isScalarValue()) {
      tokenizer.unget(token);
      parseValue(tokenizer);
    } else {
      throw errorFactory().structureError("JSON array expected");
    }
  }

  protected void parseValue(TokenIterator tokenizer) {
    throw errorFactory().structureError("JSON array expected");
  }

  public static class LenientArrayValueParser extends ArrayValueParser {

    public LenientArrayValueParser(ArrayParser arrayParser) {
      super(arrayParser);
    }

    @Override
    protected void parseValue(TokenIterator tokenizer) {
      arrayParser.elementParser().parse(tokenizer);
    }
  }
}
