/**
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
package org.apache.drill.exec.physical.impl.svremover;

import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;

public interface Copier {
  public static TemplateClassDefinition<Copier> TEMPLATE_DEFINITION2 = new TemplateClassDefinition<Copier>(Copier.class, CopierTemplate2.class);
  public static TemplateClassDefinition<Copier> TEMPLATE_DEFINITION4 = new TemplateClassDefinition<Copier>(Copier.class, CopierTemplate4.class);

  public void setupRemover(FragmentContext context, RecordBatch incoming, RecordBatch outgoing) throws SchemaChangeException;
  public abstract int copyRecords(int index, int recordCount) throws SchemaChangeException;


}