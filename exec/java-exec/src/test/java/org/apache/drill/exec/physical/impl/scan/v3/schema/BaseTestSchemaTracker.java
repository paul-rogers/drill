package org.apache.drill.exec.physical.impl.scan.v3.schema;

import java.util.Collection;

import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.EmptyErrorContext;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.v3.schema.ProjectionSchemaTracker;
import org.apache.drill.exec.physical.impl.scan.v3.schema.ScanProjectionParser;
import org.apache.drill.exec.physical.impl.scan.v3.schema.ScanProjectionParser.ProjectionParseResult;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.BaseTest;

public class BaseTestSchemaTracker extends BaseTest {

  protected static final CustomErrorContext ERROR_CONTEXT = EmptyErrorContext.INSTANCE;
  protected static final String MOCK_PROP = "my.prop";
  protected static final String MOCK_VALUE = "my-value";

  protected static final TupleMetadata SCHEMA = new SchemaBuilder()
      .add("a", MinorType.INT)
      .addNullable("b", MinorType.VARCHAR)
      .build();

  protected static final TupleMetadata MAP_SCHEMA = new SchemaBuilder()
      .add("a", MinorType.INT)
      .addMap("m")
        .add("x", MinorType.BIGINT)
        .add("y", MinorType.VARCHAR)
        .resumeSchema()
      .buildSchema();

  protected static final TupleMetadata NESTED_MAP_SCHEMA = new SchemaBuilder()
      .add("a", MinorType.INT)
      .addMap("m")
        .add("x", MinorType.BIGINT)
        .add("y", MinorType.VARCHAR)
        .addMap("m2")
          .add("p", MinorType.BIGINT)
          .add("q", MinorType.VARCHAR)
          .resumeMap()
        .resumeSchema()
      .buildSchema();

  static {
    SCHEMA.metadata("a").setProperty(MOCK_PROP, MOCK_VALUE);
    MAP_SCHEMA.metadata("m").setProperty(MOCK_PROP, MOCK_VALUE);
  }

  protected static ProjectionSchemaTracker trackerFor(Collection<SchemaPath> projList) {
    ProjectionParseResult result = ScanProjectionParser.parse(projList);
    return new ProjectionSchemaTracker(result, true, ERROR_CONTEXT);
  }
}
