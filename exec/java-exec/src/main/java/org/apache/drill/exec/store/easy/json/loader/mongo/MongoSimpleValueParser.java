package org.apache.drill.exec.store.easy.json.loader.mongo;

import org.apache.drill.exec.store.easy.json.parser.ElementParser;
import org.apache.drill.exec.store.easy.json.parser.TokenIterator;

import com.fasterxml.jackson.core.JsonToken;

/**
 * Parsers a Mongo extended type of the form:<pre><code>
 * { $type: value }</code></pre>
 */
public class MongoSimpleValueParser extends BaseMongoValueParser {

  public interface MongoValueListener {
    void onValue(JsonToken token, TokenIterator tokenizer);
  }

  private final MongoValueListener listener;

  public MongoSimpleValueParser() {

  }

  @Override
  public void parse(TokenIterator tokenizer) {

    JsonToken token = tokenizer.requireNext();
    if (token != JsonToken.START_OBJECT) {
      throw syntaxError("^{");
    }

    token = tokenizer.requireNext();
    if (token != JsonToken.FIELD_NAME ||
        !tokenizer.textValue().contentEquals(typeName()) {
      throw syntaxError( "{ ^\"%s\"");
    }

    token = tokenizer.requireNext();
    if (!token.isScalarValue()) {
      throw syntaxError( "{ \"%s\": ^scalar");
    }
    listener.onValue(token, tokenizer);

    token = tokenizer.requireNext();
    if (token != JsonToken.END_OBJECT) {
      throw syntaxError( "{ \"%s\": scalar ^}");
    }
  }

}
