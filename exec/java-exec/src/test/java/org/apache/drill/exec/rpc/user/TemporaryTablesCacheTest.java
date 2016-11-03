/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.rpc.user;

import mockit.Deencapsulation;
import org.apache.drill.exec.planner.sql.handlers.SqlHandlerUtil;
import org.apache.drill.exec.rpc.user.UserSession.TemporaryTablesCache;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.util.BiConsumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TemporaryTablesCacheTest {

  private TemporaryTablesCache temporaryTablesCache;
  private String uuid;

  @Mock
  private AbstractSchema schema;

  @Before
  public void setUp() {
    uuid = UUID.randomUUID().toString();
    temporaryTablesCache = new TemporaryTablesCache(uuid);
    when(schema.getFullSchemaName()).thenReturn("dfs.tmp");
  }

  @Test
  public void testAdd() {
    String tableName = getRandomTableName();
    temporaryTablesCache.add(schema, tableName);
    ConcurrentMap<String, ConcurrentMap<String, AbstractSchema>> cache = getCache();
    assertEquals("Temporary tables cache schema count should match", 1, cache.size());
    ConcurrentMap<String, AbstractSchema> tables = cache.get(schema.getFullSchemaName());
    assertEquals("Temporary tables cache tables count should match", 1, tables.size());
    assertEquals("Schema instance should match",
        schema, tables.get(SqlHandlerUtil.generateTemporaryTableName(tableName, uuid)));
  }

  @Test
  public void testFindPositive() {
    String tableName = getRandomTableName();
    temporaryTablesCache.add(schema, tableName);
    String temporaryTable = temporaryTablesCache.find(schema.getFullSchemaName(), tableName);
    assertEquals("Temporary table name should match",
        SqlHandlerUtil.generateTemporaryTableName(tableName, uuid), temporaryTable);
  }

  @Test
  public void testFindNegative() {
    assertNull("Temporary table should be absent",
        temporaryTablesCache.find(schema.getFullSchemaName(), getRandomTableName()));
  }

  @Test
  public void testRemoveWithoutAction() {
    String table1 = getRandomTableName();
    String table2 = getRandomTableName();
    temporaryTablesCache.add(schema, table1);
    temporaryTablesCache.add(schema, table2);
    ConcurrentMap<String, ConcurrentMap<String, AbstractSchema>> cache = getCache();
    assertEquals("Temporary tables cache should contain two items", 2, cache.get(schema.getFullSchemaName()).size());
    temporaryTablesCache.remove(schema.getFullSchemaName(), getRandomTableName(), null);
    assertEquals("Temporary tables cache should contain two items after removing absent table",
        2, cache.get(schema.getFullSchemaName()).size());
    temporaryTablesCache.remove(schema.getFullSchemaName(), table1, null);
    assertEquals("Second table should be present in cache",
        schema, cache.get(schema.getFullSchemaName()).get(SqlHandlerUtil.generateTemporaryTableName(table2, uuid)));
    assertEquals("Temporary tables cache should contain one item", 1, cache.get(schema.getFullSchemaName()).size());
    temporaryTablesCache.remove(schema.getFullSchemaName(), table2, null);
    assertTrue("Temporary tables cache should be empty", cache.get(schema.getFullSchemaName()).isEmpty());
  }

  @Test
  public void testRemoveWithAction() {
    String table = getRandomTableName();
    temporaryTablesCache.add(schema, table);
    ConcurrentMap<String, ConcurrentMap<String, AbstractSchema>> cache = getCache();
    assertEquals("Temporary tables cache should contain one item", 1, cache.get(schema.getFullSchemaName()).size());
    doNothing().when(schema).dropTable(anyString());
    temporaryTablesCache.removeAll(new BiConsumer<AbstractSchema, String>() {
      @Override
      public void accept(AbstractSchema schema, String tableName) {
        schema.dropTable(tableName);
      }
    });
    verify(schema, times(1)).dropTable(anyString());
    assertTrue("Temporary tables cache should be empty", cache.isEmpty());
  }

  @Test
  public void testRemoveAllWithoutAction() {
    String table1 = getRandomTableName();
    String table2 = getRandomTableName();
    temporaryTablesCache.add(schema, table1);
    temporaryTablesCache.add(schema, table2);
    ConcurrentMap<String, ConcurrentMap<String, AbstractSchema>> cache = getCache();
    assertEquals("Temporary tables cache should contain two items", 2, cache.get(schema.getFullSchemaName()).size());
    temporaryTablesCache.removeAll(null);
    assertTrue("Temporary tables cache should be empty", cache.isEmpty());
  }

  @Test
  public void testRemoveAllWithAction() {
    String table1 = getRandomTableName();
    String table2 = getRandomTableName();
    temporaryTablesCache.add(schema, table1);
    temporaryTablesCache.add(schema, table2);
    ConcurrentMap<String, ConcurrentMap<String, AbstractSchema>> cache = getCache();
    assertEquals("Temporary tables cache should contain two items", 2, cache.get(schema.getFullSchemaName()).size());
    doNothing().when(schema).dropTable(anyString());
    temporaryTablesCache.removeAll(new BiConsumer<AbstractSchema, String>() {
      @Override
      public void accept(AbstractSchema schema, String tableName) {
        schema.dropTable(tableName);
      }
    });
    verify(schema, times(2)).dropTable(anyString());
    assertTrue("Temporary tables cache should be empty", cache.isEmpty());
  }

  private String getRandomTableName() {
    return UUID.randomUUID().toString();
  }

  private ConcurrentMap<String, ConcurrentMap<String, AbstractSchema>> getCache() {
    return Deencapsulation.getField(temporaryTablesCache, "temporaryTables");
  }

}
