package org.apache.drill.exec.store.easy.json.structparser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class AbstractParser implements JsonElementParser {

  protected final JsonStructureParser structParser;
  private final JsonElementParser parent;
  private final String key;

  public AbstractParser(JsonStructureParser structParser, String fieldName) {
    this.structParser = structParser;
    this.parent = null;
    this.key = fieldName;
  }

  public AbstractParser(JsonElementParser parent, String fieldName) {
    this.parent = parent;
    this.structParser = parent.structParser();
    this.key = fieldName;
  }

  @Override
  public String key() { return key; }

  @Override
  public JsonElementParser parent() { return parent; }

  @Override
  public boolean isAnonymous() { return false; }

  @Override
  public JsonStructureParser structParser() { return structParser; }

  protected ErrorFactory errorFactory() {
    return structParser.errorFactory();
  }

  protected List<String> makePath() {
    JsonElementParser parser = this;
    List<String> path = new ArrayList<>();
    while (parser != null) {
      if (! parser.isAnonymous()) {
        path.add(parser.key());
      }
      parser = parser.parent();
    }
    Collections.reverse(path);
    return path;
  }

  public String fullName() {
    StringBuilder buf = new StringBuilder();
    int count = 0;
    for (String seg : makePath()) {
      if (count > 0) {
        buf.append(".");
      }
      buf.append("`");
      buf.append(seg);
      buf.append("`");
      count++;
    }
    return buf.toString();
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder()
      .append("[")
      .append(getClass().getSimpleName())
      .append(" name=")
      .append(fullName());
    if (schema() != null) {
      buf.append(", schema=")
        .append(schema().toString());
    }
    return buf.append("]").toString();
  }
}
