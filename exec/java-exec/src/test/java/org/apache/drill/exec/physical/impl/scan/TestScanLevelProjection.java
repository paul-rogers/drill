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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.NameElement;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ProjectionColumnParser;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.UnresolvedColumn;
import org.apache.drill.test.SubOperatorTest;
import org.junit.Test;

/**
 * Test the level of projection done at the level of the scan as a whole;
 * before knowledge of table "implicit" columns or the specific table schema.
 */

public class TestScanLevelProjection extends SubOperatorTest {

  @Test
  public void testColumnParserWildcard() {
    List<NameElement> cols = new ProjectionColumnParser()
        .parse(ScanTestUtils.projectList(SchemaPath.WILDCARD));
    assertEquals(1, cols.size());

    NameElement wildcard = cols.get(0);
    assertEquals(SchemaPath.WILDCARD, wildcard.name());
    assertTrue(wildcard.isSimple());
    assertTrue(wildcard.isWildcard());
    assertNull(wildcard.members());
    assertNull(wildcard.indexes());
  }

  @Test
  public void testColumnParserSimple() {
    List<NameElement> cols = new ProjectionColumnParser()
        .parse(ScanTestUtils.projectList("a", "b", "c"));
    assertEquals(3, cols.size());

    NameElement a = cols.get(0);
    assertEquals("a", a.name());
    assertTrue(a.isSimple());
    assertFalse(a.isWildcard());
    assertNull(a.members());
    assertNull(a.indexes());

    assertEquals("b", cols.get(1).name());
    assertTrue(cols.get(1).isSimple());

    assertEquals("c", cols.get(2).name());
    assertTrue(cols.get(2).isSimple());
  }

  @Test
  public void testColumnParserSimpleDups() {
    try {
      new ProjectionColumnParser()
          .parse(ScanTestUtils.projectList("a", "b", "a"));
      fail();
    } catch (UserException e) {
      // Expected
    }
  }

  @Test
  public void testColumnParserMap() {
    List<NameElement> cols = new ProjectionColumnParser()
        .parse(ScanTestUtils.projectList("a", "a.b", "a.c", "a.b.c", "d"));
    assertEquals(2, cols.size());

    NameElement a = cols.get(0);
    assertEquals("a", a.name());
    assertFalse(a.isSimple());
    assertFalse(a.isArray());
    assertTrue(a.isTuple());

    {
      List<NameElement> aMembers = a.members();
      assertEquals(2, aMembers.size());

      NameElement a_b = aMembers.get(0);
      assertEquals("b", a_b.name());
      assertTrue(a_b.isTuple());

      {
        List<NameElement> a_bMembers = a_b.members();
        assertEquals(1, a_bMembers.size());
        assertEquals("c", a_bMembers.get(0).name());
        assertTrue(a_bMembers.get(0).isSimple());
      }

      assertEquals("c", aMembers.get(1).name());
      assertTrue(aMembers.get(1).isSimple());
    }

    assertEquals("d", cols.get(1).name());
    assertTrue(cols.get(1).isSimple());
  }

  @Test
  public void testColumnParserMapDups() {
    try {
      new ProjectionColumnParser()
          .parse(ScanTestUtils.projectList("a", "a.b", "a.c", "a.b"));
      fail();
    } catch (UserException e) {
      // Expected
    }
  }

  @Test
  public void testColumnParserArray() {
    List<NameElement> cols = new ProjectionColumnParser()
        .parse(ScanTestUtils.projectList("a[1]", "a[3]"));
    assertEquals(1, cols.size());

    NameElement a = cols.get(0);
    assertEquals("a", a.name());
    assertTrue(a.isArray());
    assertFalse(a.isSimple());
    assertFalse(a.isTuple());
    boolean indexes[] = a.indexes();
    assertNotNull(indexes);
    assertEquals(4, indexes.length);
    assertFalse(indexes[0]);
    assertTrue(indexes[1]);
    assertFalse(indexes[2]);
    assertTrue(indexes[3]);
  }

  @Test
  public void testColumnParserArrayDups() {
    try {
      new ProjectionColumnParser()
          .parse(ScanTestUtils.projectList("a[1]", "a[3]", "a[1]"));
      fail();
    } catch (UserException e) {
      // Expected
    }
  }

  @Test
  public void testColumnParserArrayAndSimple() {
    try {
      new ProjectionColumnParser()
          .parse(ScanTestUtils.projectList("a[1]", "a"));
      fail();
    } catch (UserException e) {
      // Expected
    }
  }

  @Test
  public void testColumnParserSimpleAndArray() {
    try {
      new ProjectionColumnParser()
          .parse(ScanTestUtils.projectList("a", "a[1]"));
      fail();
    } catch (UserException e) {
      // Expected
    }
  }

  /**
   * Basic test: select a set of columns (a, b, c) when the
   * data source has an early schema of (a, c, d). (a, c) are
   * projected, (d) is null.
   */

  @Test
  public void testBasics() {

    // Simulate SELECT a, b, c ...
    // Build the projection plan and verify

    ScanLevelProjection scanProj = new ScanLevelProjection(
        ScanTestUtils.projectList("a", "b", "c"),
        ScanTestUtils.parsers());
    assertFalse(scanProj.projectAll());
    assertFalse(scanProj.projectNone());

    assertEquals(3, scanProj.requestedCols().size());
    assertEquals("a", scanProj.requestedCols().get(0).rootName());
    assertEquals("b", scanProj.requestedCols().get(1).rootName());
    assertEquals("c", scanProj.requestedCols().get(2).rootName());

    assertEquals(3, scanProj.columns().size());
    assertEquals("a", scanProj.columns().get(0).name());
    assertEquals("b", scanProj.columns().get(1).name());
    assertEquals("c", scanProj.columns().get(2).name());

    // Verify column type

    assertEquals(UnresolvedColumn.UNRESOLVED, scanProj.columns().get(0).nodeType());
  }

  @Test
  public void testMap() {
    ScanLevelProjection scanProj = new ScanLevelProjection(
        ScanTestUtils.projectList("a.x", "b.x", "a.y", "b.y", "c"),
        ScanTestUtils.parsers());
    assertFalse(scanProj.projectAll());
    assertFalse(scanProj.projectNone());

    assertEquals(3, scanProj.columns().size());
    assertEquals("a", scanProj.columns().get(0).name());
    assertEquals("b", scanProj.columns().get(1).name());
    assertEquals("c", scanProj.columns().get(2).name());

    // Verify column type

    assertEquals(UnresolvedColumn.UNRESOLVED, scanProj.columns().get(0).nodeType());

    // Map structure

    NameElement a = ((UnresolvedColumn) scanProj.columns().get(0)).element();
    assertTrue(a.isTuple());
    assertTrue(a.isProjected("x"));
    assertTrue(a.isProjected("y"));

    NameElement c = ((UnresolvedColumn) scanProj.columns().get(2)).element();
    assertTrue(c.isSimple());
  }

  @Test
  public void testArray() {
    ScanLevelProjection scanProj = new ScanLevelProjection(
        ScanTestUtils.projectList("a[1]", "a[3]"),
        ScanTestUtils.parsers());
    assertFalse(scanProj.projectAll());
    assertFalse(scanProj.projectNone());

    assertEquals(1, scanProj.columns().size());
    assertEquals("a", scanProj.columns().get(0).name());

    // Verify column type

    assertEquals(UnresolvedColumn.UNRESOLVED, scanProj.columns().get(0).nodeType());

    // Map structure

    NameElement a = ((UnresolvedColumn) scanProj.columns().get(0)).element();
    assertTrue(a.isArray());
    assertFalse(a.hasIndex(0));
    assertTrue(a.hasIndex(1));
    assertFalse(a.hasIndex(2));
    assertTrue(a.hasIndex(3));
  }

  /**
   * Simulate a SELECT * query by passing "*" as a column name.
   */

  @Test
  public void testWildcard() {
    ScanLevelProjection scanProj = new ScanLevelProjection(
        ScanTestUtils.projectAll(),
        ScanTestUtils.parsers());

    assertTrue(scanProj.projectAll());
    assertFalse(scanProj.projectNone());
    assertEquals(1, scanProj.requestedCols().size());
    assertTrue(scanProj.requestedCols().get(0).isWildcard());

    assertEquals(1, scanProj.columns().size());
    assertEquals(SchemaPath.WILDCARD, scanProj.columns().get(0).name());

    // Verify bindings

    assertEquals(scanProj.columns().get(0).name(), scanProj.requestedCols().get(0).rootName());

    // Verify column type

    assertEquals(UnresolvedColumn.WILDCARD, scanProj.columns().get(0).nodeType());
  }

  /**
   * Test an empty projection which occurs in a
   * SELECT COUNT(*) query.
   */

  @Test
  public void testEmptyProjection() {
    ScanLevelProjection scanProj = new ScanLevelProjection(
        ScanTestUtils.projectList(),
        ScanTestUtils.parsers());

    assertFalse(scanProj.projectAll());
    assertTrue(scanProj.projectNone());
    assertEquals(0, scanProj.requestedCols().size());
  }

  /**
   * Can't include both a wildcard and a column name.
   */

  @Test
  public void testErrorWildcardAndColumns() {
    try {
      new ScanLevelProjection(
          ScanTestUtils.projectList(SchemaPath.WILDCARD, "a"),
          ScanTestUtils.parsers());
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  /**
   * Can't include both a column name and a wildcard.
   */
  @Test
  public void testErrorColumnAndWildcard() {
    try {
      new ScanLevelProjection(
          ScanTestUtils.projectList("a", SchemaPath.WILDCARD),
          ScanTestUtils.parsers());
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }
  /**
   * Can't include a wildcard twice.
   * <p>
   * Note: Drill actually allows this, but the work should be done
   * in the project operator; scan should see at most one wildcard.
   */

  @Test
  public void testErrorTwoWildcards() {
    try {
      new ScanLevelProjection(
          ScanTestUtils.projectList(SchemaPath.WILDCARD, SchemaPath.WILDCARD),
          ScanTestUtils.parsers());
      fail();
    } catch (UserException e) {
      // Expected
    }
  }
}
