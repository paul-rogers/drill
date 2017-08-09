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

import java.util.List;

import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.physical.rowSet.model.TupleModel.RowSetModel;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractTupleWriter;

public class RowSetLoaderImpl extends AbstractTupleWriter implements RowSetLoader {

  private final WriterIndexImpl writerIndex;
  private final RowSetModel model;

  protected RowSetLoaderImpl(RowSetModel model, WriterIndexImpl index, List<AbstractObjectWriter> writers) {
    super(model.schema(), writers);
    this.model = model;
    this.writerIndex = index;
    bindIndex(index);
  }

  @Override
  public void setRow(Object...values) {
    setObject(values);
    save();
  }

  @Override
  public int rowIndex() { return writerIndex.vectorIndex(); }

  @Override
  public void save() {
    endValue();
  }

  @Override
  public boolean startRow() {
    if (! writerIndex.valid()) {
      return false;
    }
    startValue();
    return true;
  }

  @Override
  public boolean isFull( ) { return ! writerIndex.valid(); }

  @Override
  public void reset(int index) { }
}
