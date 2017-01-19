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
package org.apache.drill.exec.store.revised.exec;

import java.util.Iterator;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.store.revised.Sketch;
import org.apache.drill.exec.store.revised.Sketch.RowBatch;
import org.apache.drill.exec.store.revised.Sketch.RowSchema;

public class RowBatchImpl implements RowBatch {

  @Override
  public RowSchema schema() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int rowCount() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public BatchSchema batchSchema() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void addSv2() {
    // TODO Auto-generated method stub

  }

  @Override
  public void addSv4() {
    // TODO Auto-generated method stub

  }

  @Override
  public SelectionVector2 sv2() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SelectionVector4 sv4() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int findPath(SchemaPath path) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public TypedFieldId vectorId(int n) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public VectorWrapper<?> getVector(int n) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Iterator<VectorWrapper<?>> vectorIterator() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public VectorContainer vectorContainer() {
    // TODO Auto-generated method stub
    return null;
  }

}
