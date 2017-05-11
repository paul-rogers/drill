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
package org.apache.drill.exec.physical.impl.unorderedreceiver;

import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("requires manual verification")
public class TestValidationOptions extends ClusterTest {

  public static final String FILE_NAME = "test.csv";
  private static File testDir;

  private static String testData[] = {
      "a,b,c", "d,e,f"
  };

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    startCluster(ClusterFixture.builder()
        .maxParallelization(1)
        .sessionOption(ExecConstants.ENABLE_ITERATOR_VALIDATION, true)
        .sessionOption(ExecConstants.ENABLE_VECTOR_VALIDATION, true)
        );

    // Set up CSV storage plugin using headers.

    testDir = cluster.makeTempDir("csv");
    cluster.defineWorkspace("dfs", "data", testDir.getAbsolutePath(), "csv");

    buildFile(FILE_NAME, testData);
  }

  // To validate these tests, set breakpoints in ImplCreator
  // and IteratorValidatorBatchIterator to see if the options
  // work as expected.

  @Test
  public void testOptions() throws Exception {
    boolean hasAssertions = false;
    assert hasAssertions = true;
    assertFalse(hasAssertions);
    String sql = "SELECT * FROM `dfs.data`.`" + FILE_NAME + "`";
    client.queryBuilder().sql(sql).run();

    client.alterSession(ExecConstants.ENABLE_VECTOR_VALIDATION, false);
    client.queryBuilder().sql(sql).run();

    client.alterSession(ExecConstants.ENABLE_ITERATOR_VALIDATION, false);
    client.queryBuilder().sql(sql).run();
  }

  private static void buildFile(String fileName, String[] data) throws IOException {
    try(PrintWriter out = new PrintWriter(new FileWriter(new File(testDir, fileName)))) {
      for (String line : data) {
        out.println(line);
      }
    }
  }
}
