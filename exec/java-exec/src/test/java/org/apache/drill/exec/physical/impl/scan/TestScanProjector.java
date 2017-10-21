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
package org.apache.drill.exec.physical.impl.scan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.protocol.SchemaTracker;
import org.apache.drill.exec.physical.impl.scan.ScanTestUtils.ProjectionFixture;
import org.apache.drill.exec.physical.impl.scan.project.ScanProjector;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestScanProjector extends SubOperatorTest {
  private ProjectionFixture buildFixture(String... cols) {
    ProjectionFixture projFixture = new ProjectionFixture()
        .withFileParser(fixture.options())
        .projectedCols(cols);
    projFixture.metadataParser.useLegacyWildcardExpansion(false);
    projFixture.metadataParser.setScanRootDir(new Path("hdfs:///w"));
    projFixture.build();
    return projFixture;
  }

  private ScanProjector buildProjector(String... cols) {
    ProjectionFixture projFixture = buildFixture(cols);
    return new ScanProjector(fixture.allocator(), projFixture.scanProj, projFixture.metadataProj, null);
  }

  private ScanProjector buildProjectorWithNulls(MajorType nullType, String... cols) {
    ProjectionFixture projFixture = buildFixture(cols);
    return new ScanProjector(fixture.allocator(), projFixture.scanProj, projFixture.metadataProj, nullType);
  }
}
