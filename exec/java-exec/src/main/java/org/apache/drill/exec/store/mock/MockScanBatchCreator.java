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
package org.apache.drill.exec.store.mock;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.physical.impl.scan.ScanOperatorExec;
import org.apache.drill.exec.physical.impl.scan.ScanOperatorExec.ScanOperatorExecBuilder;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.mock.MockTableDef.MockColumn;
import org.apache.drill.exec.store.mock.MockTableDef.MockScanEntry;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class MockScanBatchCreator implements BatchCreator<MockSubScanPOP> {

  @Override
  public CloseableRecordBatch getBatch(FragmentContext context, MockSubScanPOP config, List<RecordBatch> children)
      throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    final List<MockScanEntry> entries = config.getReadEntries();
    MockScanEntry first = entries.get(0);
    if ( first.isExtended( ) ) {
      // Extended mode: use the revised, size-aware scan operator

      ScanOperatorExecBuilder builder = ScanOperatorExec.builder();
      for(final MockTableDef.MockScanEntry e : entries) {
        builder.addReader(new ExtendedMockBatchReader(e));
      }

      // If a single "entry", extract the project list. (If more than one,
      // the semantics are a bit murky, assume it is a SELECT * with each
      // entry representing a table that may have distinct schema.)

      if (entries.size() == 1) {
        for (MockColumn col : first.getTypes()) {
          builder.addProjection(col.getName());
        }
      } else {
        builder.projectAll();
      }
      return builder.buildRecordBatch(context, config);
    } else {
      final List<RecordReader> readers = Lists.newArrayList();
      for(final MockTableDef.MockScanEntry e : entries) {
        readers.add(new MockRecordReader(context, e));
      }
      return new ScanBatch(config, context, readers.iterator());
    }
  }
}
