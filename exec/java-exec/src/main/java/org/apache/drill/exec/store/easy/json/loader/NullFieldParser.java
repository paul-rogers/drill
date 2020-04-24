package org.apache.drill.exec.store.easy.json.loader;

import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl.NullTypeMarker;
import org.apache.drill.exec.store.easy.json.parser.ElementParser;
import org.apache.drill.exec.store.easy.json.parser.NullValueParser;
import org.apache.drill.exec.store.easy.json.parser.TokenIterator;

/**
 * Parser for a field that contains only nulls. Waits, consuming nulls, until
 * a non-null value appears, after which this parser is replaced by the
 * "resolved" parser.
 */
public class NullFieldParser extends NullValueParser implements NullTypeMarker {

  private final TupleParser tupleParser;

  public NullFieldParser(TupleParser tupleParser, String key) {
    super(tupleParser, key);
    this.tupleParser = tupleParser;
    tupleParser.loader().addNullMarker(this);
  }

  @Override
  public void forceResolution() {
    tupleParser.loader().removeNullMarker(this);
    tupleParser.replaceFieldParser(key, tupleParser.fieldFactory().forceNullResolution(key));
  }

  /**
   * The column type is now known from context. Create a new scalar
   * column, writer and parser to replace this parser.
   */
  @Override
  protected ElementParser resolve(TokenIterator tokenizer) {
    tupleParser.loader().removeNullMarker(this);
    return super.resolve(tokenizer);
  }
}
