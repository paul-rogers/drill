package org.apache.drill.exec.store.easy.json.parser;

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.easy.json.JsonLoader;

interface JsonElementParser {

  String key();
  JsonLoader loader();
  JsonElementParser parent();
  boolean isAnonymous();
  boolean parse();
  ColumnMetadata schema();
}
