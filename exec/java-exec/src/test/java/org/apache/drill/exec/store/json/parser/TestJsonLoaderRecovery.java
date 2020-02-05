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
package org.apache.drill.exec.store.json.parser;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.Test;

public class TestJsonLoaderRecovery extends BaseTestJsonLoader {

  /**
   * Test the JSON parser's limited recovery abilities.
   *
   * @see <a href="https://issues.apache.org/jira/browse/DRILL-4653">DRILL-4653</a>
   */
  @Test
  public void testErrorRecovery() {
    final String json = "{\"a: 10}\n{a: 20}\n{a: 30}";
    JsonFixture tester = new JsonFixture();
    tester.options.skipMalformedRecords = true;
    tester.open(json);
    final RowSet results = tester.read();

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
//        .addRow(20L) // Recovery will eat the second record.
        .addRow(30L)
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  /**
   * Test handling of unrecoverable parse errors. This test must change if
   * we resolve DRILL-5953 and allow better recovery.
   *
   * @see <a href="https://issues.apache.org/jira/browse/DRILL-5953">DRILL-5953</a>
   */

  @Test
  public void testUnrecoverableError() {
    String json = "{a: }\n{a: 20}\n{a: 30}";
    JsonFixture tester = new JsonFixture();
    tester.options.skipMalformedRecords = true;
    tester.open(json);
    expectError(tester);
    tester.close();
  }
}
