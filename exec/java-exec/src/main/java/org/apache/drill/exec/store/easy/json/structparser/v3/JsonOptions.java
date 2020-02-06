package org.apache.drill.exec.store.easy.json.structparser.v3;

public class JsonOptions {
  public boolean allTextMode;
  public boolean extended = true;
  public boolean readNumbersAsDouble;

  /**
   * Allow Infinity and NaN for float values.
   */

  public boolean allowNanInf;

  /**
   * Describes whether or not this reader can unwrap a single root array record
   * and treat it like a set of distinct records.
   */
  public boolean skipOuterList = true;
  public boolean skipMalformedRecords;
  public boolean enableEscapeAnyChar;

  /**
   * Top-level event listeners.
   */
  public ObjectListener rootListener;
  public ErrorFactory errorFactory;
}