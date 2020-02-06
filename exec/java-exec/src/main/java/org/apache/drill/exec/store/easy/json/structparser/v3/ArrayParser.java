package org.apache.drill.exec.store.easy.json.structparser.v3;

import com.fasterxml.jackson.core.JsonToken;

public class ArrayParser extends AbstractElementParser {

  private final ArrayListener arrayListener;
  private final ValueParser elementParser;

  public ArrayParser(ValueParser parent, ArrayListener arrayListener, ValueListener elementListener) {
    super(parent);
    this.arrayListener = arrayListener;
    this.elementParser = new ValueParser(this, "[]", elementListener);
  }

  public ValueParser elementParser() { return elementParser; }

  /**
   * Parses <code>[ ^ ((value)(, (value)* )? ]</code>
   */
  @Override
  public void parse(TokenIterator tokenizer) {
    arrayListener.onStart();
    top: for (;;) {
      // Position: [ (value, )* ^ ?
     JsonToken token = tokenizer.requireNext();
      switch (token) {
      case END_ARRAY:
        break top;

      default:
        tokenizer.unget(token);
        arrayListener.onElement();
        elementParser.parse(tokenizer);
        break;
      }
    }
    arrayListener.onEnd();
  }
}
