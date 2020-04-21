package org.apache.drill.exec.store.easy.json.loader.mongo;

import org.apache.drill.exec.store.easy.json.parser.ElementParser;
import org.apache.drill.exec.store.easy.json.parser.ErrorFactory;

public abstract class BaseMongoValueParser implements ElementParser {

  private final ErrorFactory errorFactory;

  protected abstract String typeName();

  protected RuntimeException syntaxError(String syntax) {
    return errorFactory.structureError(
        String.format("Expected %s for extended type $s.",
            String.format(syntax, typeName()), typeName());
  }
}
