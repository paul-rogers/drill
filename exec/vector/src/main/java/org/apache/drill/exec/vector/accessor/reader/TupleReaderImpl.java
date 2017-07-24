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

import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.vector.accessor.ArrayReader;
import org.apache.drill.exec.vector.accessor.ScalarReader;
import org.apache.drill.exec.vector.accessor.TupleReader;

/**
 * Reader for a tuple (a row or a map.) Provides access to each
 * column using either a name or a numeric index.
 */

public class TupleReaderImpl extends AbstractTupleAccessor implements TupleReader {

  private final BaseScalarReader readers[];

  public TupleReaderImpl(TupleMetadata schema, BaseScalarReader readers[]) {
    super(schema);
    this.readers = readers;
  }

  @Override
  public ScalarReader column(int colIndex) {
    return readers[colIndex];
  }

  @Override
  public ScalarReader column(String colName) {
    int index = schema.index(colName);
    if (index == -1) {
      return null; }
    return readers[index];
  }
}
