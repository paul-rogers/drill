package org.apache.drill.exec.store.easy.json.structparser.v3;


public abstract class AbstractElementParser implements ElementParser {
  final JsonStructureParser structParser;
  private final ElementParser parent;

  public AbstractElementParser(ElementParser parent) {
    this.parent = parent;
    this.structParser = parent.structParser();
  }

  @Override
  public ElementParser parent() { return parent; }

  @Override
  public JsonStructureParser structParser() { return structParser; }

  protected ErrorFactory errorFactory() {
    return structParser.errorFactory();
  }
}