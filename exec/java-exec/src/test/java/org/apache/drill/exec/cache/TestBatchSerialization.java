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
package org.apache.drill.exec.cache;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.test.DrillTest;
import org.apache.drill.test.OperatorFixture;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSet.ExtendableRowSet;
import org.apache.drill.test.rowSet.RowSet.RowSetWriter;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBatchSerialization extends DrillTest {

  public static OperatorFixture fixture;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    fixture = OperatorFixture.builder().build();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    fixture.close();
  }

  public SingleRowSet makeRowSet(BatchSchema schema, int rowCount) {
    ExtendableRowSet rowSet = fixture.rowSet(schema);
    RowSetWriter writer = rowSet.writer(rowCount);
    for (int i = 0; i < rowCount; i++) {
      writer.next();
      RowSetUtilities.setFromInt(writer, 0, i);
    }
    writer.done();
    return rowSet;
  }

  public void testType(MinorType type) throws IOException {
    BatchSchema schema = new SchemaBuilder( )
        .add("col", type)
        .build();
    int rowCount = 20;
    SingleRowSet rowSet = makeRowSet(schema, rowCount);

    File dir = OperatorFixture.getTempDir("serial");
    File outFile = new File(dir, type.toString() + ".dat");
    try (OutputStream out = new BufferedOutputStream(new FileOutputStream(outFile))) {
      VectorSerializer.writer(fixture.allocator(), out)
        .write(rowSet.getContainer());
    }

    RowSet result;
    try (InputStream in = new BufferedInputStream(new FileInputStream(outFile))) {
      result = fixture.wrap(
        VectorSerializer.reader(fixture.allocator(), in)
          .read());
    }

    new RowSetComparison(makeRowSet(schema, rowCount))
      .verifyAndClear(result);
    outFile.delete();
  }

  @Test
  public void testTypes() throws IOException {
    testType(MinorType.TINYINT);
    testType(MinorType.UINT1);
    testType(MinorType.SMALLINT);
    testType(MinorType.UINT2);
    testType(MinorType.INT);
    testType(MinorType.UINT4);
    testType(MinorType.BIGINT);
    testType(MinorType.UINT8);
    testType(MinorType.FLOAT4);
    testType(MinorType.FLOAT8);
    testType(MinorType.DECIMAL9);
    testType(MinorType.DECIMAL18);
    testType(MinorType.DECIMAL28SPARSE);
    testType(MinorType.DECIMAL38SPARSE);
//  testType(MinorType.DECIMAL28DENSE); No writer
//  testType(MinorType.DECIMAL38DENSE); No writer
    testType(MinorType.DATE);
    testType(MinorType.TIME);
    testType(MinorType.TIMESTAMP);
    testType(MinorType.INTERVAL);
    testType(MinorType.INTERVALYEAR);
    testType(MinorType.INTERVALDAY);
  }
}
