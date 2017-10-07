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
 * Writer for a Drill "union vector." The union vector is presented
 * as a writer over a set of variants. In the old Visual Basic world,
 * "the Variant data type is a tagged union that can be used to
 * represent any other data type." The term is used here to avoid
 * confusion with the "union operator" which is something entirely
 * different.
 * <p>
 * At write time, the set of possible types is expanded upon the
 * first write to each type. A request to obtain a writer for type
 * will create the underlying storage vector if needed, then return
 * a write of the proper type. Note that unlike most other writers,
 * the caller is <i>required</i> to call the
 * {@link #scalar(MinorType)} method for each value so that this
 * writer knows which type of value is to be stored.
 * <p>
 * This writer acts as somewhat like a map: it allows access to
 * type-specific writers.
 * <p>
 * Although the union and list vectors supports a union of any Drill
 * type, the only sane combinations are:
 * <ul>
 * <li>One of a (single or repeated) (map or list), or</li>
 * <li>One or more scalar type.</li>
 * </ul>
 *
 * @see {@link VariantReader}
 */

public interface VariantWriter {

  interface VariantWriterListener {
    ObjectWriter addType(MinorType type);
  }

  void bindListener(VariantWriterListener listener);

  /**
   * Returns the inferred type of this variant. Valid only after the
   * value is known: either from context (for maps and lists) or after
   * a value is written (for scalars)
   *
   * @return the type of this variant as an object type
   */

  ObjectType valueType();

  /**
   * Determine if the union vector has materialized storage for the
   * given type. (The storage will be created as needed during writing.)
   *
   * @param type data type
   * @return <tt>true</tt> if a value of the given type has been written
   * and storage allocated (or storage was allocated implicitly),
   * <tt>false</tt> otherwise
   */

  boolean hasType(MinorType type);

  /**
   * A scalar variant may be null. In this case, the value is not a
   * null of some type; rather it is simply null.
   */

  void setNull();

  ObjectWriter writer(MinorType type);
  ScalarWriter scalar(MinorType type);
  TupleWriter tuple();
  ArrayWriter array();

  /**
   * Write the given object into the backing vector, creating a vector
   * of the required type if needed. Primarily for testing.
   *
   * @param value value to write to the vector. The Java type of the
   * object indicates the Drill storage type
   */

  void setObject(Object value);
}
