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
package org.apache.drill.test.rowSet;

import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.vector.accessor.reader.AbstractObjectReader;
import org.apache.drill.exec.vector.accessor.reader.AbstractTupleReader;
import org.apache.drill.test.rowSet.AbstractRowSet.RowSetReaderIndex;

/**
 * Reader implementation for a row set.
 */

public class RowSetReaderImpl extends AbstractTupleReader implements RowSetReader {

  protected final RowSetReaderIndex index;

  public RowSetReaderImpl(TupleMetadata schema, RowSetReaderIndex index, AbstractObjectReader[] readers) {
    super(schema, readers);
    this.index = index;
  }

  @Override
  public boolean next() {
    if (! index.next()) {
      return false;
    }
    reposition();
    return true;
  }

  @Override
  public boolean valid() { return index.valid(); }

  @Override
  public int index() { return index.position(); }

  @Override
  public int rowCount() { return index.size(); }

  @Override
  public int rowIndex() { return index.vectorIndex(); }

  @Override
  public int batchIndex() { return index.batchIndex(); }

  @Override
  public void set(int index) { this.index.set(index); }
}
