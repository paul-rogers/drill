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
package org.apache.drill.exec.physical.impl.xsort;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.util.DrillStringUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.proto.UserBitShared.QueryData;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.util.Text;
import org.apache.drill.test.BufferingQueryEventListener;
import org.apache.drill.test.BufferingQueryEventListener.QueryEvent;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.DrillTest;
import org.apache.drill.test.FixtureBuilder;
import org.apache.drill.test.ProfileParser;
import org.apache.drill.test.ProfileParser.OperatorProfile;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.apache.drill.exec.physical.impl.xsort.managed.ExternalSortBatch;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.DrillBuf;

public class TestManagedExternalSort extends DrillTest {

  private static ClusterFixture cluster;
  private static ClientFixture client;

  @BeforeClass
  public static void setup() throws Exception {
    cluster = builder().build();
    client = cluster.clientFixture();
  }

  @AfterClass
  public static void shutDown() throws Exception {
    client.close();
    cluster.close();
  }

  @Test
  public void testSingleIntKey() throws Exception {
    String sql = "SELECT id_i FROM `mock`.employee_100 ORDER BY id_i";
    BufferingQueryEventListener results = client.queryBuilder()
      .sql(sql)
      .withEventListener();
    new VerifierBuilder(client)
      .intKey("id_i", VerifierBuilder.SortDirection.ASC)
      .rows(100)
      .build()
      .verify(results);
  }

  @Test
  public void testDoubleIntKey() throws Exception {
    String sql = "SELECT a_i, b_i FROM `mock`.employee_100 ORDER BY b_i, a_i";
    BufferingQueryEventListener results = client.queryBuilder()
      .sql(sql)
      .withEventListener();
    new VerifierBuilder(client)
      .intKey("b_i", VerifierBuilder.SortDirection.ASC)
      .intKey("a_i", VerifierBuilder.SortDirection.ASC)
      .rows(100)
      .build()
      .verify(results);
  }

  @Test
  public void testDoubleIntKeyMixed() throws Exception {
    String sql = "SELECT a_i, b_i FROM `mock`.employee_100 ORDER BY b_i DESC, a_i ASC";
    BufferingQueryEventListener results = client.queryBuilder()
      .sql(sql)
      .withEventListener();
    new VerifierBuilder(client)
      .intKey("b_i", VerifierBuilder.SortDirection.DESC)
      .intKey("a_i", VerifierBuilder.SortDirection.ASC)
      .rows(100)
      .build()
      .verify(results);
  }

  @Test
  public void testSingleIntKeyLarge() throws Exception {
    String sql = "SELECT id_i FROM `mock`.employee_10M ORDER BY id_i";
    BufferingQueryEventListener results = client.queryBuilder()
      .sql(sql)
      .withEventListener();
    new VerifierBuilder(client)
      .intKey("id_i", VerifierBuilder.SortDirection.ASC)
      .rows(10_000_000)
      .build()
      .verify(results);
  }

  @Test
  public void testSingleStringKey() throws Exception {
    String sql = "SELECT name_s100 FROM `mock`.employee_100 ORDER BY name_s100";
    BufferingQueryEventListener results = client.queryBuilder()
      .sql(sql)
      .withEventListener();
    new VerifierBuilder(client)
      .stringKey("name_s100", VerifierBuilder.SortDirection.ASC)
      .rows(100)
      .build()
      .verify(results);
  }

  @Test
  public void testDoubleStringKey() throws Exception {
    String sql = "SELECT a_s100, b_s200 FROM `mock`.employee_100 ORDER BY b_s200, a_s100";
    BufferingQueryEventListener results = client.queryBuilder()
      .sql(sql)
      .withEventListener();
    new VerifierBuilder(client)
      .stringKey("b_s200", VerifierBuilder.SortDirection.ASC)
      .stringKey("a_s100", VerifierBuilder.SortDirection.ASC)
      .rows(100)
      .build()
      .verify(results);
  }

  @Test
  public void testDoubleKeyMixed() throws Exception {
    String sql = "SELECT a_s100, b_i FROM `mock`.employee_100 ORDER BY b_i DESC, a_s100 ASC";
    BufferingQueryEventListener results = client.queryBuilder()
      .sql(sql)
      .withEventListener();
    new VerifierBuilder(client)
      .intKey("b_i", VerifierBuilder.SortDirection.DESC)
      .stringKey("a_s100", VerifierBuilder.SortDirection.ASC)
      .rows(100)
      .build()
      .verify(results);
  }

  @Test
  public void testSingleStringKeyBig() throws Exception {
    String sql = "SELECT name_s10000 FROM `mock`.employee_1M ORDER BY name_s10000";
    BufferingQueryEventListener results = client.queryBuilder()
      .sql(sql)
      .withEventListener();
    SortVerifier verifier = new VerifierBuilder(client)
      .stringKey("name_s10000", VerifierBuilder.SortDirection.ASC)
      .rows(1_000_000)
      .build();
    verifier.verify(results);

    // Use the profile to verify that spilling occurred.

    ProfileParser profile = client.parseProfile(verifier.queryId());
    List<OperatorProfile> ops = profile.getOpsOfType(CoreOperatorType.EXTERNAL_SORT_VALUE);
    assertEquals(1, ops.size());
    OperatorProfile sort = ops.get(0);
    long spillCount = sort.getMetric(ExternalSortBatch.Metric.SPILL_COUNT.ordinal());
    long mergeCount = sort.getMetric(ExternalSortBatch.Metric.MERGE_COUNT.ordinal());
    System.out.println(String.format("Spills: %d, merge/spills: %d", spillCount, mergeCount));
    assertTrue(spillCount > 0);
    assertTrue(mergeCount > 0);
  }

  public static class VerifierBuilder {
    enum SortDirection { ASC, DESC };

    private List<String> keys = new ArrayList<>();
    private List<KeyFieldVerifier> verifiers = new ArrayList<>();
    private ClientFixture client;
    private int expectedRows = -1;

    public VerifierBuilder(ClientFixture client) {
      this.client = client;
    }

    public VerifierBuilder rows(int n) {
      expectedRows = n;
      return this;
    }

    public VerifierBuilder intKey(String name, SortDirection dir) {
      keys.add(name);
      verifiers.add(new IntFieldVerifier(dir == SortDirection.ASC));
      return this;
    }

    public VerifierBuilder stringKey(String name, SortDirection dir) {
      keys.add(name);
      verifiers.add(new StringFieldVerifier(dir == SortDirection.ASC));
      return this;
    }

    public SortVerifier build() {
      assertTrue("No keys", ! keys.isEmpty());
      String keyArray[] = new String[keys.size()];
      keys.toArray(keyArray);
      KeyFieldVerifier verifierArray[] = new KeyFieldVerifier[keys.size()];
      verifiers.toArray(verifierArray);
      KeyVerifierImpl keyVerifier = new KeyVerifierImpl(keyArray, verifierArray);
      return new SortVerifier(client, keyVerifier, expectedRows);
    }
  }

  public interface KeyVerifier {

    String[] keys();
    void reset();
    void verify(int batchCount, int rowCount, Object[] keyValues);
  }

  public static class KeyVerifierImpl implements KeyVerifier {

    String[] keys;
    KeyFieldVerifier fieldVerifiers[];

    public KeyVerifierImpl(String keys[], KeyFieldVerifier[] verifiers) {
      this.keys = keys;
      this.fieldVerifiers = verifiers;
    }

    @Override
    public String[] keys() {
      return keys;
    }

    @Override
    public void reset( ) {
      for (int i = 0; i < fieldVerifiers.length; i++) {
        fieldVerifiers[i].reset();
      }
    }

    @Override
    public void verify(int batchNo, int rowNo, Object[] keyValues) {
      assert fieldVerifiers.length == keyValues.length;
      boolean reset = false;
      for (int i = 0; i < keyValues.length; i++ ) {
        KeyFieldVerifier verifier = fieldVerifiers[i];
        if (reset) {
          verifier.reset();
        }
        KeyFieldVerifier.KeyResult result = verifier.verify(keyValues[i]);
        switch (result) {
        case CHANGE:
          reset = true;
          break;
        case INVALID:
          fail(String.format("Batch %d, row %d, key %s - value out of order. " +
                             "Prev value = %s, current value = %s",
                             batchNo, rowNo, keys[i],
                             verifier.asString( verifier.prevValue() ),
                             verifier.asString( keyValues[i] )));
          break;
        case SAME:
          break;
        default:
          assert false;
          break;
        }
      }
    }
  }

  public interface KeyFieldVerifier {
    public enum KeyResult { SAME, CHANGE, INVALID };

    void reset();
    KeyResult verify(Object keyValue);
    Object prevValue();
    String asString(Object value);
  }

  public static abstract class AbstractFieldVerifier implements KeyFieldVerifier {

    protected final boolean isAsc;

    public AbstractFieldVerifier(boolean asc) {
      this.isAsc = asc;
    }

    @Override
    public String asString(Object value) {
      return value.toString();
    }
  }

  public static class IntFieldVerifier extends AbstractFieldVerifier {
    private int prevValue;

    public IntFieldVerifier(boolean asc) {
      super(asc);
    }

    @Override
    public void reset() {
      prevValue = isAsc ? Integer.MIN_VALUE : Integer.MAX_VALUE;
    }

    @Override
    public KeyResult verify(Object keyValue) {
      int value = (Integer) keyValue;
      if (value == prevValue) {
        return KeyResult.SAME;
      }
      boolean ok = isAsc ? (value >= prevValue) : (value <= prevValue);
      prevValue = value;
      return ok ? KeyResult.CHANGE : KeyResult.INVALID;
    }

    @Override
    public Object prevValue() {
      return prevValue;
    }
  }

  public static class StringFieldVerifier extends AbstractFieldVerifier {
    private String prevValue;
    private boolean first;

    public StringFieldVerifier(boolean asc) {
      super(asc);
    }

    @Override
    public void reset() {
      prevValue = null;
      first = true;
    }

    @Override
    public KeyResult verify(Object keyValue) {
      Text text = (Text) keyValue;
      String value = text.toString();
      if (first) {
        first = false;
        prevValue = value;
        return KeyResult.CHANGE;
      }
      int result = compare(prevValue, value);
      if (isAsc) {
        result = -result;
      }
      if (result < 0) {
        return KeyResult.INVALID;
      } else if (result == 0) {
        return KeyResult.SAME;
      } else {
        return KeyResult.CHANGE;
      }
    }

    private static int compare(String s1, String s2) {
      // Assume nulls low for now

      if (s1 == null && s2 == null) { return 0; }
      if (s1 == null) { return -1; }
      if (s2 == null) { return 1; }
      return s1.compareTo(s2);
    }

    @Override
    public Object prevValue() {
      return prevValue;
    }
  }

  public static class SortVerifier {

    String queryId;
    private ClientFixture client;
    RecordBatchLoader loader;
    Map<String,Integer> keyMap = new HashMap<>();
    private String[] keys;
    private int batchCount;
    private int rowCount;
    private KeyVerifier keyVerifier;
    private int expectedCount;

    public SortVerifier(ClientFixture client, KeyVerifier verifier, int expectedCount) {
      this.client = client;
      loader = new RecordBatchLoader(client.allocator());
      this.keys = verifier.keys();
      this.keyVerifier = verifier;
      this.expectedCount = expectedCount;
      for (int i = 0; i < keys.length; i++) {
        keyMap.put(keys[i], i);
      }
    }

    public String queryId() {
      return queryId;
    }

    public void verify(BufferingQueryEventListener results) {
      keyVerifier.reset();
      outer:
      for ( ; ; ) {
        QueryEvent event = results.get();
        switch (event.type) {
        case BATCH:
          verifyBatch(event.batch);
          break;
        case EOF:
          break outer;
        case ERROR:
          fail(event.error.getMessage());
          break;
        case QUERY_ID:
          queryId = QueryIdHelper.getQueryId(event.queryId);
          break;
        default:
          assert false;
          break;
        }
      }
      if (expectedCount != -1) {
        assertEquals("Row count", expectedCount, rowCount);
      }
    }

    private void verifyBatch(QueryDataBatch batch) {
      try {
        final QueryData header = batch.getHeader();
        @SuppressWarnings("resource")
        final DrillBuf data = batch.getData();
        if (data != null) {
          try {
            loader.load(header.getDef(), data);
          } catch (SchemaChangeException e) {
            fail(e.getMessage());
          }
          try {
            verifyResultBatch(loader);
          } finally {
            loader.clear();
          }
          batchCount++;
        }
      } finally {
        batch.release();
      }
    }

    private void verifyResultBatch(VectorAccessible va) {
      int columnCount = 0;
      for (@SuppressWarnings("unused") VectorWrapper<?> vw : va) {
        columnCount++;
      }
      assertTrue("Not enough columns", columnCount >= keys.length);
      int keyIndexes[] = new int[columnCount];
      for (int i = 0; i < columnCount; i++) {
        keyIndexes[i] = -1;
      }
      int colIndex = 0;
      for (VectorWrapper<?> vw : va) {
        String colPath = vw.getValueVector().getField().getPath();
        Integer keyPosn = keyMap.get(colPath);
        if ( keyPosn != null ) {
          keyIndexes[colIndex] = keyPosn;
        }
        colIndex++;
      }
      VectorWrapper<?> keyVws[] = new VectorWrapper<?>[keys.length];
      colIndex = 0;
      for (VectorWrapper<?> vw : va) {
        if (keyIndexes[colIndex] != -1) {
          keyVws[keyIndexes[colIndex]] = vw;
        }
        colIndex++;
      }
      for (int i = 0; i < keys.length; i++) {
        if (keyVws[i] == null) {
          fail("Column not found for key: " + keys[i] );
        }
      }
      int rows = va.getRecordCount();
      Object keyValues[] = new Object[keys.length];
      for (int row = 0; row < rows; row++) {
        for (int i = 0; i < keys.length; i++) {
          keyValues[i] = keyVws[i].getValueVector().getAccessor().getObject(row);
        }
        keyVerifier.verify( batchCount, rowCount, keyValues );
        rowCount++;
      }
    }

  }

  private static FixtureBuilder builder() {
    return ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
//        .configProperty(ExecConstants.REMOVER_ENABLE_GENERIC_COPIER, true)
//        .configProperty(ExecConstants.EXTERNAL_SORT_BATCH_LIMIT, 40)
        .configProperty(ExecConstants.EXTERNAL_SORT_MERGE_LIMIT, 40)
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .configProperty(ExecConstants.EXTERNAL_SORT_MAX_MEMORY, 20_000_000)
//        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 3L * 1024 * 1024 * 1024)
        .maxParallelization(1)
        ;
  }

}
