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
package org.apache.drill.exec.physical.impl.xsort.managed;

import java.io.IOException;

import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.vector.CopyUtil;

public class OperatorCodeGenerator {

  private static final GeneratorMapping COPIER_MAPPING = new GeneratorMapping("doSetup", "doCopy", null, null);
  private static final MappingSet COPIER_MAPPING_SET = new MappingSet(COPIER_MAPPING, COPIER_MAPPING);

  private final ExternalSortBatch esb;
  private final FragmentContext context;
  private BatchSchema schema;

  /**
   * A single PriorityQueueCopier instance is used for 2 purposes:
   * 1. Merge sorted batches before spilling
   * 2. Merge sorted batches when all incoming data fits in memory
   */

  private PriorityQueueCopier copier;

  public OperatorCodeGenerator( ExternalSortBatch esb, FragmentContext context ) {
    this.esb = esb;
    this.context = context;
  }

  public void setSchema( BatchSchema schema ) {
    close( );
    this.schema = schema;
  }

  public void close( ) {
    closeCopier( );
    copier = null;
  }

  public void closeCopier( ) {
    if ( copier != null ) {
      try {
        copier.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public PriorityQueueCopier getCopier( VectorAccessible batch ) {
    if ( copier == null ) {
      copier = generateCopier( batch );
    }
    return copier;
  }

  private PriorityQueueCopier generateCopier( VectorAccessible batch ) {
    // Generate the copier code and obtain the resulting class

    CodeGenerator<PriorityQueueCopier> cg = CodeGenerator.get(PriorityQueueCopier.TEMPLATE_DEFINITION, context.getFunctionRegistry(), context.getOptions());
    ClassGenerator<PriorityQueueCopier> g = cg.getRoot();

    try {
      esb.generateComparisons(g, batch);
    } catch (SchemaChangeException e) {
      throw new RuntimeException("Unexpected schema change", e);
    }

    g.setMappingSet(COPIER_MAPPING_SET);
    CopyUtil.generateCopies(g, batch, true);
    g.setMappingSet(ExternalSortBatch.MAIN_MAPPING);
    try {
      return context.getImplementationClass(cg);
    } catch (ClassTransformationException | IOException e) {
      throw new RuntimeException(e);
    }
  }
}
