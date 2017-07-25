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
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;

/**
 * Column writer implementation that acts as the basis for the
 * generated, vector-specific implementations. All set methods
 * throw an exception; subclasses simply override the supported
 * method(s).
 */

public abstract class BaseScalarWriter extends AbstractScalarWriter {

  protected ColumnWriterIndex vectorIndex;
  protected int lastWriteIndex;

  public static ScalarObjectWriter build(ColumnWriterIndex vectorIndex,
                              ValueVector vector, BaseScalarWriter writer) {
    writer.bind(vectorIndex, vector);
    return new ScalarObjectWriter(writer);
  }

  protected void bind(ColumnWriterIndex vectorIndex) {
    this.vectorIndex = vectorIndex;
  }

  public abstract void bind(ColumnWriterIndex rowIndex, ValueVector vector);

  @Override
  public void startWrite() { lastWriteIndex = -1; }
}
