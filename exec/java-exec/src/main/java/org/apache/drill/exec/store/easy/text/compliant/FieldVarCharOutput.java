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
package org.apache.drill.exec.store.easy.text.compliant;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.rowSet.ColumnLoader;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.LoaderSchema;

/**
 * Class is responsible for generating record batches for text file inputs. We generate
 * a record batch with a set of varchar vectors. A varchar vector contains all the field
 * values for a given column. Each record is a single value within each vector of the set.
 */
class FieldVarCharOutput extends BaseFieldOutput {

  private final int maxField;

  /**
   * We initialize and add the varchar vector for each incoming field in this
   * constructor.
   * @param outputMutator  Used to create/modify schema
   * @param fieldNames Incoming field names
   * @param columns  List of columns selected in the query
   * @param isStarQuery  boolean to indicate if all fields are selected or not
   * @throws SchemaChangeException
   */
  public FieldVarCharOutput(ResultSetLoader loader) {
    super(loader);

    LoaderSchema schema = writer.schema();
    int end = schema.columnCount() - 1;
    if (schema.hasProjection()) {
      while (end >= 0 & schema.metadata(end).isProjected()) {
        end--;
      }
      if (end == -1) {
        throw new IllegalStateException("No columns selected");
      }
    }
    maxField = end;
  }

  @Override
  public boolean endField() {
    super.endField();

    ColumnLoader colLoader = writer.column(currentFieldIndex);
    if (colLoader != null) {
      colLoader.setBytes(fieldBytes, currentDataPointer);
    }

    return currentFieldIndex < maxField;
  }
}
