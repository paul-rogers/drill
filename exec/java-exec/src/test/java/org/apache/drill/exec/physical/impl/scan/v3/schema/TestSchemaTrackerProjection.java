package org.apache.drill.exec.physical.impl.scan.v3.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;

import org.apache.drill.categories.EvfTests;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.EmptyErrorContext;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.v3.schema.ScanProjectionParser.ProjectionParseResult;
import org.apache.drill.exec.physical.resultSet.impl.ProjectionFilter;
import org.apache.drill.exec.physical.rowSet.RowSetTestUtils;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.BaseTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test the first step of scan schema resolution: translating from the
 * projection parser to a dynamic schema ready for resolution.
 */
@Category(EvfTests.class)
public class TestSchemaTrackerProjection extends BaseTest {
  private static final CustomErrorContext ERROR_CONTEXT = EmptyErrorContext.INSTANCE;

  private ProjectionSchemaTracker schemaTracker(Collection<SchemaPath> projList) {
    ProjectionParseResult result = ScanProjectionParser.parse(projList);
    return new ProjectionSchemaTracker(result, true, EmptyErrorContext.INSTANCE);
  }

  @Test
  public void testEmpty() {
    ProjectionSchemaTracker tracker = schemaTracker(
        Collections.emptyList());
    assertTrue(tracker.isResolved());
    assertEquals(0, tracker.schemaVersion());
    assertSame(ScanSchemaTracker.ProjectionType.NONE, tracker.projectionType());
    assertTrue(tracker.schema().toSchema().isEmpty());
    ProjectionFilter filter = tracker.projectionFilter(ERROR_CONTEXT);
    assertSame(ProjectionFilter.PROJECT_NONE, filter);
  }

  @Test
  public void testWildcard() {
    ProjectionSchemaTracker tracker = schemaTracker(
        RowSetTestUtils.projectAll());
    assertFalse(tracker.isResolved());
    assertEquals(0, tracker.schemaVersion());
    assertSame(ScanSchemaTracker.ProjectionType.ALL, tracker.projectionType());
    assertTrue(tracker.schema().toSchema().isEmpty());
    ProjectionFilter filter = tracker.projectionFilter(ERROR_CONTEXT);
    assertSame(ProjectionFilter.PROJECT_ALL, filter);
  }

  @Test
  public void testWildcardAndCols() {
    ProjectionSchemaTracker tracker = schemaTracker(
        RowSetTestUtils.projectList("a", SchemaPath.DYNAMIC_STAR, "b"));
    assertFalse(tracker.isResolved());
    assertTrue(0 < tracker.schemaVersion());
    assertSame(ScanSchemaTracker.ProjectionType.ALL, tracker.projectionType());
    TupleMetadata schema = tracker.schema().toSchema();
    assertEquals(2, schema.size());
    assertTrue(schema.metadata(0).isDynamic());
    ProjectionFilter filter = tracker.projectionFilter(ERROR_CONTEXT);
    assertTrue(filter instanceof DynamicSchemaFilter);
  }

  @Test
  public void testExplicit() {
    ProjectionSchemaTracker tracker = schemaTracker(
        RowSetTestUtils.projectList("a", "b", "c"));
    assertSame(ScanSchemaTracker.ProjectionType.SOME, tracker.projectionType());
    assertTrue(0 < tracker.schemaVersion());
    TupleMetadata schema = tracker.schema().toSchema();
    assertEquals(3, schema.size());
    assertTrue(schema.metadata(0).isDynamic());
    ProjectionFilter filter = tracker.projectionFilter(ERROR_CONTEXT);
    assertTrue(filter instanceof DynamicSchemaFilter);
  }
}
