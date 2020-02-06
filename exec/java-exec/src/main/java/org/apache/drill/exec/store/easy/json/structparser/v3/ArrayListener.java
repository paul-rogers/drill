package org.apache.drill.exec.store.easy.json.structparser.v3;

/**
 * Represents one level within an array. The first time the parser sees
 * the array element, it will call one of the "Element" methods with the
 * look-ahead values visible to the parser. Since JSON is flexible, later
 * data shapes may not necessarily follow the first shape. The implementation
 * must handle this or throw an error if not supported.
 * <p>
 * When creating a multi-dimensional array, each array level is built one
 * by one. each will receive the same type information (decreased by one
 * array level.)
 * <p>
 * Then, while parsing, the parser calls events on the start and end of the
 * array, as well as on each element.
 * <p>
 * The array listener is an attribute of a value listener, represent the
 * "arrayness" of that value, if the value allows an array.
 */
public interface ArrayListener {

  void onStart();
  void onElement();
  void onEnd();

  ValueListener objectArrayElement(int arrayDims);
  ValueListener objectElement();
  ValueListener arrayElement(int arrayDims, JsonType type);
  ValueListener scalarElement(JsonType type);
}
