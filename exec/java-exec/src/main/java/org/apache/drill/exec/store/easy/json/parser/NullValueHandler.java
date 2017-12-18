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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl.JsonElementParser;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;

public class NullValueHandler {

  interface NullTypeMarker {
    boolean isEmptyArray();
    void realizeAsText();
  }

  private final JsonLoaderImpl loader;

  // Using a simple list. Won't perform well if we have hundreds of
  // null fields; but then we've never seen such a pathologically bad
  // case... Usually just one or two fields have deferred nulls.

  private final List<NullValueHandler.NullTypeMarker> nullStates = new ArrayList<>();

  public NullValueHandler(JsonLoaderImpl loader) {
    this.loader = loader;
  }

  public void add(NullValueHandler.NullTypeMarker marker) {
    nullStates.add(marker);
  }

  public void remove(NullValueHandler.NullTypeMarker marker) {
    nullStates.remove(marker);
  }

  public void endBatch() {
    List<NullValueHandler.NullTypeMarker> copy = new ArrayList<>();
    copy.addAll(nullStates);
    for (NullValueHandler.NullTypeMarker marker : copy) {
      MajorType type = null;
      if (loader.options.typeNegotiator != null) {
        type = loader.options.typeNegotiator.typeOf(makePath((JsonElementParser) marker));
      }
      if (type == null) {
        marker.realizeAsText();
      } else {
        realizeAsType(marker, type);
      }
    }
    assert nullStates.isEmpty();
  }

  private static List<String> makePath(JsonElementParser parser) {
    List<String> path = new ArrayList<>();
    while (parser != null) {
      if (! parser.isAnonymous()) {
        path.add(parser.key());
      }
      parser = parser.parent();
    }
    Collections.reverse(path);
    return path;
  }

  private void realizeAsType(NullValueHandler.NullTypeMarker marker, MajorType type) {
    JsonElementParser state = (JsonElementParser) marker;
    MinorType dataType = type.getMinorType();
    if (marker.isEmptyArray()) {
      if (type.getMode() != DataMode.REPEATED) {
        marker.realizeAsText();
      } else {
        realizeAsArray(marker, state, dataType);
      }
    } else {
      switch (type.getMode()) {
      case OPTIONAL:
        realizeAsScalar(marker, state, dataType);
        break;
      case REPEATED:
        realizeAsArray(marker, state, dataType);
        break;
      default:
        marker.realizeAsText();
      }
    }
  }

  private void realizeAsArray(NullValueHandler.NullTypeMarker marker, JsonElementParser state, MinorType type) {
    ObjectParser tupleState = (ObjectParser) state.parent();
    ArrayWriter arrayWriter = tupleState.newWriter(state.key(), type, DataMode.REPEATED).array();
    ScalarWriter scalarWriter = arrayWriter.scalar();
    JsonElementParser elementState = stateForType(type, tupleState, state.key(), scalarWriter);
    JsonLoaderImpl.logger.warn("Ambiguous type! JSON array {} contains all empty arrays. " +
        "Assuming element type from prior file: {}",
        state.key(), type.toString());
    JsonElementParser newState = new ArrayParser.ScalarArrayParser(tupleState, state.key(), arrayWriter, elementState);
    tupleState.replaceChild(state.key(), newState);
    nullStates.remove(marker);
  }

  private void realizeAsScalar(NullValueHandler.NullTypeMarker marker, JsonElementParser state, MinorType type) {
    ObjectParser tupleState = (ObjectParser) state.parent();
    ScalarWriter scalarWriter = tupleState.newWriter(state.key(), type, DataMode.OPTIONAL).scalar();
    JsonElementParser newState = stateForType(type, tupleState, state.key(), scalarWriter);
    JsonLoaderImpl.logger.warn("Ambiguous type! JSON field {} contains all nulls. " +
        "Assuming element type from prior file: {}",
        state.key(), type.toString());
    tupleState.replaceChild(state.key(), newState);
    nullStates.remove(marker);
  }

  private JsonElementParser stateForType(MinorType type, JsonElementParser parent, String fieldName, ScalarWriter scalarWriter) {
    switch (type) {
    case BIGINT:
      return new ScalarParser.IntParser(parent, fieldName, scalarWriter);
    case FLOAT8:
      return new ScalarParser.FloatParser(parent, fieldName, scalarWriter);
    case TINYINT:
      return new ScalarParser.BooleanParser(parent, fieldName, scalarWriter);
    case VARCHAR:
      return new ScalarParser.StringParser(parent, fieldName, scalarWriter);
    default:
      throw new IllegalStateException("Unsupported Drill type " + type.toString() + " for JSON array");
    }
  }

}

