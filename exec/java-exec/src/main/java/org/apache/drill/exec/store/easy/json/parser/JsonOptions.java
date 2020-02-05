package org.apache.drill.exec.store.easy.json.parser;

public class JsonOptions {
  public String context;
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
  public boolean unionEnabled;

  public TupleProjection rootProjection;

  /**
   * By default, the JSON parser uses repeated types: either a repeated
   * scalar, repeated map or repeated list. If this flag is enabled, the
   * parser will use the experimental list vector type which can form
   * a list of any type (including lists), and allows array values to
   * be null. The code for lists existed in bits and pieces in Drill 1.12,
   * was extended to work for JSON in Drill 1.13, but remains to be
   * supported in the rest of Drill.
   */

  public boolean useListType;
  public boolean detectTypeEarly;
  public boolean enableEscapeAnyChar;
}