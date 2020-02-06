package org.apache.drill.exec.store.easy.json.structparser.v3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonToken;

public abstract class RootParser implements ElementParser {
  protected static final Logger logger = LoggerFactory.getLogger(RootParser.class);

  private final JsonStructureParser structParser;
  protected final ObjectParser rootObject;

  public RootParser(JsonStructureParser structParser) {
    this.structParser = structParser;
    this.rootObject = new ObjectParser(this, structParser.options().rootListener);
  }

  public abstract boolean parseRoot(TokenIterator tokenizer);

  @Override
  public void parse(TokenIterator tokenizer) {
    throw new UnsupportedOperationException();
  }

  protected boolean parseRootObject(JsonToken token, TokenIterator tokenizer) {
    // Position: ^ ?
    switch (token) {
      case NOT_AVAILABLE:
        return false; // Should never occur

      case START_OBJECT:
        // Position: { ^
        rootObject.parse(tokenizer);
        break;

      default:
        // Position ~{ ^
        // Not a valid object.
        throw errorFactory().syntaxError(token); // Nothing else is valid
    }
    return true;
  }

  protected ErrorFactory errorFactory() {
    return structParser.errorFactory();
  }


  @Override
  public ElementParser parent() { return null; }

  @Override
  public JsonStructureParser structParser() { return structParser; }

  public static class RootObjectParser extends RootParser {

    public RootObjectParser(JsonStructureParser structParser) {
      super(structParser);
    }

    @Override
    public boolean parseRoot(TokenIterator tokenizer) {
      JsonToken token = tokenizer.next();
      if (token == null) {
        // Position: EOF ^
        return false;
      }
      return parseRootObject(token, tokenizer);
    }
  }

  public static class RootArrayParser extends RootParser {

    public RootArrayParser(JsonStructureParser structParser) {
      super(structParser);
    }

    @Override
    public boolean parseRoot(TokenIterator tokenizer) {
      JsonToken token = tokenizer.next();
      if (token == null) {
        // Position: { ... EOF ^
        // Saw EOF, but no closing ]. Warn and ignore.
        logger.warn("Failed to close outer array. {}",
            tokenizer.context());
        return false;
      }
      if (token == JsonToken.END_ARRAY) {
        return false;
      }
      return parseRootObject(token, tokenizer);
    }
  }
}