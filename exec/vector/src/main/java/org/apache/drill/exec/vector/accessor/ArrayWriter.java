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

import org.apache.drill.exec.vector.VectorOverflowException;
import org.apache.drill.exec.vector.accessor.ObjectWriter.ObjectType;

/**
 * Writer for values into an array. Array writes are write-once,
 * sequential: each call to a <tt>setFoo()</tt> method writes a
 * value and advances the array index.
 * <p>
 * {@see ArrayReader}
 */

public interface ArrayWriter {
  int size();

  ObjectWriter entry();
  ObjectType entryType();
  ScalarWriter scalar();
  TupleWriter tuple();
  ArrayWriter array();

  void set(Object ...values) throws VectorOverflowException;
  void setArray(Object array) throws VectorOverflowException;
//  void setList(List<? extends Object> list);
}
