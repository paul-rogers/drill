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
package org.apache.drill.exec.physical.impl.scan.framework;

import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.vector.ValueVector;

public abstract class AbstractSchemaNegotiatorImpl implements SchemaNegotiator {

  private final OperatorContext context;
  protected TupleMetadata tableSchema;
  protected int batchSize = ValueVector.MAX_ROW_COUNT;

  public AbstractSchemaNegotiatorImpl(OperatorContext context) {
    this.context = context;
  }

  @Override
  public OperatorContext context() {
    return context;
  }

  @Override
  public void setTableSchema(TupleMetadata schema) {
    tableSchema = schema;
  }

  @Override
  public void setBatchSize(int maxRecordsPerBatch) {
    batchSize = maxRecordsPerBatch;
  }
}
