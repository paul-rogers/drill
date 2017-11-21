/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.vector.accessor;

import org.apache.drill.common.types.TypeProtos.MinorType;

/**
 * Reader for a Drill "union vector." The union vector is presented
 * as a reader over a set of variants. In the old Visual Basic world,
 * "the Variant data type is a tagged union that can be used to
 * represent any other data type." The term is used here to avoid
 * confusion with the "union operator" which is something entirely
 * different.
 * <p>
 * At read time, the set of possible types is fixed. A request to
 * obtain a reader for an unused type returns a null pointer.
 * <p>
 * This reader acts as somewhat like a map: it allows access to
 * type-specific readers for the set of types supported in the
 * current vector. A client checks the type of each value, then
 * uses the proper type-specific reader to access that value.
 *
 * @see {@link VariantWriter}
 */

public interface VariantReader {

  /**
   * Determine if a given type is supported by the union vector
   * for some value in the result set.
   *
   * @param type the Drill minor type to query
   *
   * @return <tt>true</tt> if a reader for the given type is available,
   * <tt>false</tt> if a request for a reader of that type will return
   * <tt>null</tt>.
   */

  boolean hasType(MinorType type);

  /**
   * Determine if the current value is null. Null values have no type
   * and no associated reader.
   *
   * @return <tt>true</tt> if this value is <tt>NULL</tt>,
   * <tt>false</tt> if the entry has a value
   */

  boolean isNull();

  /**
   * Return the data type of the current value. (What happens if the row is
   * null, must it be a null of some type?)
   *
   * @return data type of the current data value
   */

  MinorType dataType();

  ObjectReader reader(MinorType type);
  ObjectReader reader();
  ScalarReader scalar(MinorType type);

  /**
   * Return the appropriate scalar reader for the current value.
   *
   * @return <tt>null</tt> if {@link #isNull()} returns <tt>true</tt>,
   * else the equivalent of {@link #scalar(MinorType) scalar}(
   * {@link #dataType()} )
   *
   * @throws IllegalStateException if called for a variant that
   * holds a tuple or an array
   */

  ScalarReader scalar();
  TupleReader tuple();
  ArrayReader array();

  /**
   * Return the value as a Java object of the correct type.
   *
   * @return <tt>null</tt> if {@link #isNull()} returns <tt>true</tt>,
   * else the equivalent of {@link #scalar(MinorType) scalar}().
   * {@link ScalarReader#getObject() getObject()}
   */

  Object getObject();
  String getAsString();
}
