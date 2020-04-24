package org.apache.drill.exec.store.easy.json.loader;

import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl.NullTypeMarker;
import org.apache.drill.exec.store.easy.json.parser.ElementParser;
import org.apache.drill.exec.store.easy.json.parser.EmptyArrayParser;
import org.apache.drill.exec.store.easy.json.parser.TokenIterator;

import com.fasterxml.jackson.core.JsonToken;

/**
 * Represents a run of empty arrays for which we have no type information.
 * Resolves to an actual type when a non-empty array appears. Must resolve
 * the array even if it contains nulls so we can count the null values.
 */
public class EmptyArrayFieldParser extends EmptyArrayParser implements NullTypeMarker {

  private final TupleParser tupleParser;

  public EmptyArrayFieldParser(TupleParser tupleParser, String key) {
    super(tupleParser, key);
    this.tupleParser = tupleParser;
    tupleParser.loader().addNullMarker(this);
  }

  @Override
  public void forceResolution() {
    tupleParser.loader().removeNullMarker(this);
    tupleParser.replaceFieldParser(key, resolveNullElement());
  }

  /**
   * The column type is now known from context. Create a new array
   * column, writer and parser to replace this parser.
   */
  @Override
  protected ElementParser resolve(JsonToken token, TokenIterator tokenizer) {
    tupleParser.loader().removeNullMarker(this);
    return super.resolve(token, tokenizer);
  }

  @Override
  protected ElementParser resolveNullElement() {
    return tupleParser.fieldFactory().forceArrayResolution(key);
  }
}
