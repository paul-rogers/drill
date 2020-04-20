package org.apache.drill.exec.store.easy.json.loader.mongo;

/**
 * Names of Mongo extended types. Includes both
 * <a href="https://docs.mongodb.com/manual/reference/mongodb-extended-json-v1/">
 * V1</a> and
 * <a href="https://docs.mongodb.com/manual/reference/mongodb-extended-json/">
 * V2</a> names.
 *
 * @see org.apache.drill.exec.vector.complex.fn.ExtendedTypeName ExtendedTypeName
 * for an older version of these names
 */
public interface ExtendedTypeNames {
  String LONG = "$numberLong";
}
