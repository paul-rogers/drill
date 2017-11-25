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
package org.apache.drill.exec.vector.accessor.reader;

import java.util.List;

import org.apache.drill.exec.record.TupleMetadata;

/**
 * Reader for a Drill Map type. Maps are actually tuples, just like rows.
 */

public class MapReader extends AbstractTupleReader {

  protected MapReader(TupleMetadata schema, AbstractObjectReader readers[]) {
    super(schema, readers);
  }

  public static TupleObjectReader build(TupleMetadata schema, AbstractObjectReader readers[]) {
    MapReader mapReader = new MapReader(schema, readers);
    mapReader.bindNullState(NullStateReader.REQUIRED_STATE_READER);
    return new TupleObjectReader(mapReader);
  }

  public static AbstractObjectReader build(TupleMetadata metadata,
      List<AbstractObjectReader> readers) {
    AbstractObjectReader readerArray[] = new AbstractObjectReader[readers.size()];
    return build(metadata, readers.toArray(readerArray));
  }
}
