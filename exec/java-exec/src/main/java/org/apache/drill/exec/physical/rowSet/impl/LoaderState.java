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

import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl.VectorContainerBuilder;
import org.apache.drill.exec.physical.rowSet.model.single.AbstractSingleTupleModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.MapColumnModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.PrimitiveColumnModel;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractTupleWriter;
import org.apache.drill.exec.vector.accessor.writer.TupleConstructor;

public interface LoaderState {

  public interface TupleState extends LoaderState, TupleConstructor {

  }

  public interface RootState extends TupleState {

  }

  public interface MapState extends TupleState {

  }

  public interface ColumnState extends LoaderState {

  }

  public interface PrimitiveColumnState extends ColumnState {

  }

  public interface MapColumnState extends ColumnState {

  }

  public interface StateVisitor {
    void visitRoot(RootState root);
    void visitMap(MapState map);
    void visitProjectedPrimitive(PrimitiveColum)
  }

//  void rollOver(int overflowIndex);
//  void resetBatch();
//  void harvest();
//  void buildContainer(VectorContainerBuilder containerBuilder);
//  void reset();
//  void reset(int index);
//  void close();

}
