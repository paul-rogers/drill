package org.apache.drill.exec.store.easy.json.structparser;

interface JsonElementParser {
  String key();
  JsonElementParser parent();
  boolean parse(TokenIterator tokenizer);
  JsonStructureParser structParser();
  boolean isAnonymous();
}
