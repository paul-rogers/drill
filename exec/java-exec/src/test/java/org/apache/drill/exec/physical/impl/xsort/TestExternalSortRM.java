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
package org.apache.drill.exec.physical.impl.xsort;

import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.drill.BufferingQueryEventListener;
import org.apache.drill.BufferingQueryEventListener.QueryEvent;
import org.apache.drill.ClusterFixture;
import org.apache.drill.ClusterFixture.FixtureBuilder;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.compile.ClassBuilder;
import org.apache.drill.exec.compile.CodeCompiler;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.test.DrillTest;
import org.junit.Test;

public class TestExternalSortRM extends DrillTest {

//  @Test
//  public void testManagedSpilled() throws Exception {
//    LogAnalyzer analyzer = new LogAnalyzer( true );
//    analyzer.setupLogging( );
//
//    FixtureBuilder builder = ClusterFixture.builder()
//        .property(ExecConstants.EXTERNAL_SORT_BATCH_LIMIT, 40)
//        .property(ExecConstants.EXTERNAL_SORT_MERGE_LIMIT, 40)
//        .maxParallelization(1);
//    try (ClusterFixture cluster = builder.build()) {
//      cluster.defineWorkspace( "dfs", "data", "/Users/paulrogers/work/data", "psv" );
//      performSort( cluster.client() );
//    }
//
//    EventAnalyzer analysis = analyzer.analyzeLog( );
//    SortStats stats = analysis.getStats( );
//
//    // Verify that spilling occurred. That it occurred
//    // correctly is verified by the query itself.
//
//    assertTrue( stats.gen1SpillCount > 0 );
//    assertTrue( stats.gen2SpillCount > 0 );
//    analysis.report( );
//  }

  @Test
  public void testManagedSpilledWide() throws Exception {
//    LogAnalyzer analyzer = new LogAnalyzer( true );
//    analyzer.setupLogging( );

    FixtureBuilder builder = ClusterFixture.builder()
//        .property(ClassBuilder.SAVE_CODE_OPTION, true)
      .property(CodeCompiler.DISABLE_CACHE_CONFIG, true)
        .property(ClassBuilder.CODE_DIR_OPTION, "/tmp/code")
        .maxParallelization(1);
    try (ClusterFixture cluster = builder.build()) {
      cluster.defineWorkspace( "dfs", "data", "/Users/paulrogers/work/data", "psv" );
      String sql = "select * from (select *, row_number() over(order by validitydate) as rn from `dfs.data`.`gen.json`) where rn=10";
      String plan = cluster.queryPlan(sql);
      System.out.println( plan );
      for ( int i = 0;  i < 10;  i++ ) {
        performSort( cluster.client(), sql );
      }
    }

//    analyzer.analyzeLog( );
  }

//  @Test
//  public void testLegacySpilled() throws Exception {
//    LogAnalyzer analyzer = new LogAnalyzer( false );
//    analyzer.setupLogging( );
//
//    FixtureBuilder builder = ClusterFixture.builder()
//        .configResource("xsort/drill-external-sort-legacy.conf")
//        .maxParallelization(1);
//    try (ClusterFixture cluster = builder.build()) {
//      cluster.defineWorkspace( "dfs", "data", "/Users/paulrogers/work/data", "psv" );
//      performSort( cluster.client() );
//    }
//
//    analyzer.analyzeLog( );
//  }
//
//  @Test
//  public void testManagedInMemory() throws Exception {
//    LogAnalyzer analyzer = new LogAnalyzer( true );
//    analyzer.setupLogging( );
//
//    FixtureBuilder builder = ClusterFixture.builder()
//        .maxParallelization(1);
//    try (ClusterFixture cluster = builder.build()) {
//      cluster.defineWorkspace( "dfs", "data", "/Users/paulrogers/work/data", "psv" );
//      performSort( cluster.client() );
//    }
//
//    analyzer.analyzeLog( );
//  }

  private int performSort(DrillClient client, String sql) throws IOException {
    BufferingQueryEventListener listener = new BufferingQueryEventListener( );
    long start = System.currentTimeMillis();
    client.runQuery(QueryType.SQL, sql, listener);
    int recordCount = 0;
    int batchCount = 0;
    loop:
    for ( ; ; ) {
      QueryEvent event = listener.get();
      switch ( event.type )
      {
      case BATCH:
        batchCount++;
        recordCount += event.batch.getHeader().getRowCount();
        event.batch.release();
        break;
      case EOF:
        break loop;
      case ERROR:
        event.error.printStackTrace();
        fail( );
        break loop;
      case QUERY_ID:
        break;
      default:
        break;
      }
    }
    long end = System.currentTimeMillis();
    long elapsed = end - start;

    System.out.println(String.format("Sorted %,d records in %d batches; %d ms.", recordCount, batchCount, elapsed));
    return recordCount;
  }

//  private void performSort(DrillClient client) throws IOException {
//    int recordCount = performSort( client,
//        Files.toString(FileUtils.getResourceAsFile("/xsort/sort-big-all.sql"),
//        Charsets.UTF_8) );
//    assertEquals(2880404, recordCount);
//  }
}
