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
package org.apache.drill.exec.store.revised.retired;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.revised.exec.AbstractColumnMaker;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.NullableIntVector;

public abstract class VectorBuilder extends AbstractColumnMaker {

  ScanState scanState;

  public void bind(ScanState scanState) {
    this.scanState = scanState;
  }

  public abstract void build(OutputMutator batchMutator, MaterializedField field) throws SchemaChangeException;

  public static class IntVectorBuilder extends VectorBuilder
  {
    private IntVector vector;
    IntVector.Mutator mutator;

    @Override
    public void setInt(int value) {
      mutator.set(scanState.rowIndex, value);
    }

    @Override
    public void build(OutputMutator batchMutator, MaterializedField field) throws SchemaChangeException {
      vector = batchMutator.addField(field, IntVector.class);
      mutator = vector.getMutator();
    }

  }

  public static class NullableIntVectorBuilder extends VectorBuilder
  {
    private NullableIntVector vector;
    NullableIntVector.Mutator mutator;

    @Override
    public void setInt(int value) {
      mutator.set(scanState.rowIndex, value);
    }


    @Override
    public void setNull() {
      mutator.setNull(scanState.rowIndex);
    }

    @Override
    public void build(OutputMutator batchMutator, MaterializedField field) throws SchemaChangeException {
      vector = batchMutator.addField(field, NullableIntVector.class);
      mutator = vector.getMutator();
    }
  }

  public static class VectorBuilderFactory {

    public static VectorBuilder newBuilder(MajorType majorType) {
      DataMode mode = majorType.getMode();
      switch( majorType.getMinorType() ) {
      case INT:
        switch(mode) {
        case REQUIRED:
          return new IntVectorBuilder( );
        case OPTIONAL:
          return new NullableIntVectorBuilder( );
        default:
          break;
        }
        break;
      default:
        break;
      }
      throw new IllegalStateException("Not yet supported: " + majorType.getMinorType() );
    }

  }

}
