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
package org.apache.drill.exec.store.easy.json.parser;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.physical.resultSet.project.RequestedTuple;
import org.apache.drill.exec.physical.resultSet.project.RequestedTuple.RequestedColumn;

public class TupleProjectionImpl {

  private static final TupleProjection FULL_PROJECTION = new ImplicitProjection(true);
  private static final TupleProjection EMPTY_PROJECTION = new ImplicitProjection(false);

  public static class ImplicitProjection implements TupleProjection {
    private final boolean projectAll;

    public ImplicitProjection(boolean projectAll) {
      this.projectAll = projectAll;
    }

    @Override
    public boolean isProjected(String key) { return projectAll; }

    @Override
    public MajorType typeOf(String key) { return null; }

    @Override
    public TupleProjection map(String key) { return this; }

    @Override
    public Hint typeHint(String key) { return null; }
  }

  public static class RequestedProjectionShim implements TupleProjection {
    private final RequestedTuple requestedTuple;

    public RequestedProjectionShim(RequestedTuple requestedTuple) {
      this.requestedTuple = requestedTuple;
    }

    @Override
    public boolean isProjected(String key) {
      return requestedTuple.get(key) != null;
    }

    @Override
    public MajorType typeOf(String key) { return null; }

    @Override
    public TupleProjection map(String key) {
      RequestedTuple mapProj = requestedTuple.mapProjection(key);
      switch (mapProj.type()) {
        case ALL:
          return projectAll();
        case NONE:
          return projectNone();
        default:
          return new RequestedProjectionShim(mapProj);
      }
    }

    @Override
    public Hint typeHint(String key) {
      RequestedColumn col = requestedTuple.get(key);
      if (col == null) {
        return null;
      }
      switch (col.type()) {
        case ARRAY:
          return Hint.ARRAY;
        case DICT:
        case TUPLE:
          return Hint.MAP;
        case DICT_ARRAY:
        case TUPLE_ARRAY:
          return Hint.MAP_ARRAY;
        default:
          return null;
      }
    }
  }

  public static TupleProjection projectAll() { return FULL_PROJECTION; }

  public static TupleProjection projectNone() { return EMPTY_PROJECTION; }

  public static TupleProjection projectionFor(RequestedTuple requestedTuple) {
    return new RequestedProjectionShim(requestedTuple);
  }
}
