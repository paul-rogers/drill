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
package org.apache.drill.exec.physical.rowSet.impl;

import org.apache.drill.exec.physical.rowSet.ArrayLoader;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.impl.AbstractColumnWriter;

/**
 * Represents an array (repeated) column. An array column returns an array
 * loader to handle writing. Thus, there are two parts: the column itself,
 * the value of which is an array. The array is a structure, so another
 * loader writes the array members.
 */

public class ArrayColumnLoader extends AbstractStructuredLoader {

  /**
   * Represents each array member. Wraps a generated repeated column writer.
   */

  public class ArrayMemberLoader extends AbstractScalarLoader implements ArrayLoader {

    private final ArrayWriter arrayWriter;

    protected ArrayMemberLoader(WriterIndexImpl index, ArrayWriter writer) {
      super(index, writer);
      arrayWriter = writer;
    }

    @Override
    public int size() { return arrayWriter.size(); }

    @Override
    public void setNull() {
      throw new UnsupportedOperationException();
    }
  }

  private final AbstractColumnWriter writer;
  private final ArrayMemberLoader member;

  public ArrayColumnLoader(WriterIndexImpl writerIndex, AbstractColumnWriter writer) {
    this.writer = writer;
    member = new ArrayMemberLoader(writerIndex, writer.array());
  }

  @Override
  public int writeIndex() { return writer.lastWriteIndex(); }

  @Override
  public void reset() { writer.reset(); }

  @Override
  public void resetTo(int dest) { writer.reset(dest); }

  @Override
  public ArrayLoader array() { return member; }
}
