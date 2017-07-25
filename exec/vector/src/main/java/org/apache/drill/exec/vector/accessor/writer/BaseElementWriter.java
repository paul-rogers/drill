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
package org.apache.drill.exec.vector.accessor.writer;

import org.apache.drill.exec.vector.ValueVector;

/**
 * Writer for an array-valued column. This writer appends values: once a value
 * is written, it cannot be changed. As a result, writer methods have no item index;
 * each set advances the array to the next position. This is an abstract base class;
 * subclasses are generated for each repeated value vector type.
 */

public abstract class BaseElementWriter extends AbstractScalarWriter {

  protected ElementWriterIndex vectorIndex;

  protected void bind(ElementWriterIndex vectorIndex) {
    this.vectorIndex = vectorIndex;
  }

  public abstract void bind(ElementWriterIndex rowIndex, ValueVector vector);
}