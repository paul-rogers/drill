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
package org.apache.drill.exec.store.revised.proto;

import java.util.Map;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.revised.proto.ProtoPlugin.ProtoSubScanPop;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.ValueVector;

public class ProtoRecordReader implements RecordReader {

  private ValueVector[] valueVectors;
  private int recordCount;
  private int batchSize;
  private int targetCount;
  private FragmentContext context;
  private ProtoSubScanPop config;

  public ProtoRecordReader(FragmentContext context, ProtoSubScanPop config) {
    this.config = config;
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output)
      throws ExecutionSetupException {
    valueVectors = new ValueVector[1];
    final MajorType type = MajorType.newBuilder()
        .setMinorType(MinorType.INT)
        .setMode(DataMode.OPTIONAL)
        .build();
    final MaterializedField field = MaterializedField.create("num", type);
    final Class<? extends ValueVector> vvClass = TypeHelper.getValueVectorClass(field.getType().getMinorType(), field.getDataMode());
    try {
      valueVectors[0] = output.addField(field, vvClass);
    } catch (SchemaChangeException e) {
      throw new IllegalStateException(e);
    }
    recordCount = 0;
    batchSize = 1000;
    targetCount = 10;
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap)
      throws OutOfMemoryException {
    try {
      for (final ValueVector v : vectorMap.values()) {
        AllocationHelper.allocate(v, Character.MAX_VALUE, 50, 10);
      }
    } catch (NullPointerException e) {
      throw new OutOfMemoryException();
    }
  }

  @Override
  public int next() {
    if (recordCount >= targetCount) {
      return 0;
    }

    final int recordSetSize = Math.min(batchSize, targetCount - recordCount);
    int n = recordCount + 1;
    for ( int i = 0;  i < recordSetSize; i++ ) {
      for (final ValueVector v : valueVectors) {
        final ValueVector.Mutator m = v.getMutator();
        final NullableIntVector.Mutator im = (NullableIntVector.Mutator) m;
        im.set(i, n++);
      }
    }

    recordCount += recordSetSize;
    return recordSetSize;
  }
}
