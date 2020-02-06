package org.apache.drill.exec.store.easy.json.structparser.v3;

/**
 * Parser for a JSON element. Parsers are structured in a hierarchy:
 * <ul>
 * <li>Root - handles top-level objects and arrays, as well as EOF
 * detection.</li>
 * <li>Object - Parses {@code field: value} pairs.</li>
 * <li>Value - Parses a value, which may be an array or an object.</li>
 * <li>Array - Nested within a Value; parses one level of an array.
 * Its children are Values (which may contain more array levels.</li>
 * <li>
 * JSON is completely generic; the element parsers handle JSON's full
 * flexibility. Listeners attached to each parser determine if the
 * actual value in any position makes sense for the structure being
 * parsed.
 */
public interface ElementParser {
  ElementParser parent();
  JsonStructureParser structParser();
  void parse(TokenIterator tokenizer);
}
