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

public class LoaderStateImpl {

  public static class BaseTupleState implements TupleConstructor {

    private final AbstractSingleTupleModel model;

    @Override
    public AbstractObjectWriter addColumn(ColumnMetadata column,
        AbstractTupleWriter writer) {
      // TODO Auto-generated method stub
      return null;
    }
  }

  public static class RootState extends BaseTupleState {

  }

  public static class MapState extends BaseTupleState {

  }

  public static class ColumnState {

  }

  public static class PrimitiveColumnState extends ColumnState {

    private final PrimitiveColumnModel model;

    public PrimitiveColumnState(PrimitiveColumnModel model) {
      this.model = model;
    }
  }

  public static class ProjectedPrimitiveColumnState extends PrimitiveColumnState {

    public ProjectedPrimitiveColumnState(PrimitiveColumnModel model) {
      super(model);
    }
  }

  public static class UnprojectedPrimitiveColumnState extends PrimitiveColumnState {

    public UnprojectedPrimitiveColumnState(PrimitiveColumnModel model) {
      super(model);
    }
  }

  public static class MapColumnState extends ColumnState {

    private final MapColumnModel model;

    public MapColumnState(MapColumnModel model) {
      this.model = model;
    }

  }

  public static class ProjectedMapColumnState extends MapColumnState {

    public ProjectedMapColumnState(MapColumnModel model) {
      super(model);
    }

  }

  public static class UnprojectedMapColumnState extends MapColumnState {

    public UnprojectedMapColumnState(MapColumnModel model) {
      super(model);
    }

  }

  public static class StateVisitor {
    void visitRoot(RootState root) { }
    void visitMap(MapState map) { }
    void visitProjectedPrimitive(ProjectedPrimitiveColumnState column) { }
    void visitUnprojectedPrimitive(UnprojectedPrimitiveColumnState column) { }
    void visitProjectedMapColumn(ProjectedMapColumnState mapColumn) { }
    void visitUnprojectedMapColumn(UnprojectedMapColumnState mapColumn) { }
  }
}
