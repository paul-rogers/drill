package org.apache.drill.exec.physical.impl.scan.project;

import java.util.Collection;
import java.util.Map;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.project.ProjectionSet.ColumnReadProjection;
import org.apache.drill.exec.physical.impl.scan.project.ProjectionSet.CustomTypeTransform;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple.RequestedColumn;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple.TupleProjectionType;
import org.apache.drill.exec.physical.rowSet.project.RequestedTupleImpl;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.convert.ColumnConversionFactory;
import org.apache.drill.exec.vector.accessor.convert.StandardConversions;
import org.apache.drill.exec.vector.accessor.convert.StandardConversions.ConversionDefn;
import org.apache.drill.exec.vector.accessor.convert.StandardConversions.ConversionType;

import com.google.common.base.Preconditions;

public class Exp {

  public static abstract class AbstractReadColProj implements ColumnReadProjection {
    protected final ColumnMetadata readSchema;
    protected final RequestedColumn requestedCol;
    protected final ColumnMetadata outputSchema;

    public AbstractReadColProj(ColumnMetadata readSchema) {
      this(readSchema, null, null);
    }

    public AbstractReadColProj(ColumnMetadata readSchema, RequestedColumn requestedCol) {
      this(readSchema, requestedCol, null);
    }

    public AbstractReadColProj(ColumnMetadata readSchema, ColumnMetadata outputSchema) {
      this(readSchema, null, outputSchema);
    }

    public AbstractReadColProj(ColumnMetadata readSchema, RequestedColumn requestedCol, ColumnMetadata outputSchema) {
      this.readSchema = readSchema;
      this.requestedCol = requestedCol;
      this.outputSchema = outputSchema;
      if (requestedCol != null) {
        requestedCol.type().validateProjection(readSchema);
      }
    }

    @Override
    public ColumnMetadata inputSchema() { return readSchema; }

    @Override
    public boolean isProjected() { return true; }

    @Override
    public ColumnConversionFactory conversionFactory() { return null; }

    @Override
    public ColumnMetadata outputSchema() { return outputSchema; }

    @Override
    public RequestedTuple mapProjection() {
      return requestedCol == null ? null : requestedCol.mapProjection();
    }
  }

  /**
   * Unprojected column. No validation needed. No type conversion.
   * Reader column just "free wheels", without a materialized vector,
   * accepting any data the reader cares to throw at it, then simply
   * discarding that data.
   */

  public static class UnprojectedReadColProj extends AbstractReadColProj {

    public UnprojectedReadColProj(ColumnMetadata readSchema) {
      super(readSchema);
    }

    @Override
    public boolean isProjected() { return false; }
  }

  /**
   * Column projected via a wildcard, without an output schema. All
   * columns allowed, materialized with the type given by the reader.
   * No type transforms needed. No explicit projection available to
   * validate reader types.
   */

  public static class WildcardReadColProj extends AbstractReadColProj {

    public WildcardReadColProj(ColumnMetadata readSchema) {
      super(readSchema);
    }
  }

  /**
   * Projected column based on an explicit projection, which may provide
   * constraints for the type of column allowed. No type conversion
   * needed: output type is whatever the reader chooses.
   */
  public static class ExplicitReadColProj extends AbstractReadColProj {

    public ExplicitReadColProj(ColumnMetadata col, RequestedColumn reqCol) {
      super(col, reqCol);
    }
  }

  /**
   * Projected column backed by a provided column schema and optionally
   * a projected column request.
   * Both the explicit projection and the provided schema constrain the
   * reader column types allowed. The provided schema may trigger a
   * type conversion.
   */

  public static class SchemaReadColProj extends AbstractReadColProj {

    ColumnConversionFactory conversionFactory;

    public SchemaReadColProj(ColumnMetadata readSchema, RequestedColumn reqCol,
        ColumnMetadata outputSchema, ColumnConversionFactory conversionFactory) {
      super(readSchema, reqCol, outputSchema);
      this.conversionFactory = conversionFactory;
    }
  }

  public static abstract class AbstractProjectionSet implements ProjectionSet {

    public static boolean isSpecial(ColumnMetadata col) {
      return col.getBooleanProperty(ColumnMetadata.EXCLUDE_FROM_WILDCARD);
    }
  }

  public static class WildcardProjectionSet extends AbstractProjectionSet {

    @Override
    public ColumnReadProjection readProjection(ColumnMetadata col) {
      if (isSpecial(col)) {
        return new UnprojectedReadColProj(col);
      } else {
        return new WildcardReadColProj(col);
      }
    }
  }

  public static abstract class AbstractSchemaProjectionSet extends AbstractProjectionSet {
    private static final org.slf4j.Logger logger =
        org.slf4j.LoggerFactory.getLogger(AbstractSchemaProjectionSet.class);

    protected final TupleMetadata outputSchema;
    private final CustomTypeTransform customTransform;
    protected final Map<String, String> properties;

    public AbstractSchemaProjectionSet(TupleMetadata outputSchema,
        Map<String, String> properties, CustomTypeTransform customTransform) {
      this.outputSchema = outputSchema;
      this.customTransform = customTransform;
      this.properties = properties;
    }

    protected SchemaReadColProj schemaReadProj(ColumnMetadata inputSchema, RequestedColumn reqCol, ColumnMetadata outputCol) {
      return new SchemaReadColProj(inputSchema, reqCol, outputCol,
          conversionFactory(inputSchema, outputCol));
    }

    private ColumnConversionFactory conversionFactory(ColumnMetadata inputSchema, ColumnMetadata outputCol) {

      ConversionDefn defn = StandardConversions.analyze(inputSchema, outputCol);
      if (defn.type == ConversionType.NONE) {
        return null;
      }
      if (customTransform != null) {
        ColumnConversionFactory factory = customTransform.transform(inputSchema, outputCol, defn);
        if (factory != null) {
          return factory;
        }
      }
      if (defn.type == ConversionType.IMPLICIT) {
        return null;
      }
      if (defn.conversionClass == null) {
        throw UserException.validationError()
          .message("Runtime type conversion not available")
          .addContext("Input type", inputSchema.typeString())
          .addContext("Output type", outputCol.typeString())
          .build(logger);
      }
      return StandardConversions.factory(defn.conversionClass, properties);
    }
  }

  public static class WildcardAndSchemaProjectionSet extends AbstractSchemaProjectionSet {

    private final boolean isStrict;

    public WildcardAndSchemaProjectionSet(TupleMetadata outputSchema,
        Map<String, String> properties, CustomTypeTransform customTransform) {
      super(outputSchema, properties, customTransform);
      isStrict = outputSchema.getBooleanProperty(TupleMetadata.IS_STRICT_SCHEMA_PROP);
    }

    @Override
    public ColumnReadProjection readProjection(ColumnMetadata col) {
      if (isSpecial(col)) {
        return new UnprojectedReadColProj(col);
      }

      ColumnMetadata outputCol = outputSchema.metadata(col.name());
      if (outputCol == null) {
        if (isStrict) {
          return new UnprojectedReadColProj(col);
        } else {
          return new WildcardReadColProj(col);
        }
      }
      if (isSpecial(outputCol)) {
        return new UnprojectedReadColProj(col);
      }
      return schemaReadProj(col, null, outputCol);
    }
  }

  public static class ExplicitProjectionSet implements ProjectionSet {

    private final RequestedTuple requestedProj;

    public ExplicitProjectionSet(RequestedTuple requestedProj) {
      this.requestedProj = requestedProj;
    }

    @Override
    public ColumnReadProjection readProjection(ColumnMetadata col) {
      RequestedColumn reqCol = requestedProj.get(col.name());
      if (reqCol == null) {
        return new UnprojectedReadColProj(col);
      }
      return new ExplicitReadColProj(col, reqCol);
    }

  }

  public static class ExplicitSchemaProjectionSet extends AbstractSchemaProjectionSet {

    private final RequestedTuple requestedProj;

    public ExplicitSchemaProjectionSet(RequestedTuple requestedProj, TupleMetadata outputSchema,
        Map<String, String> properties, CustomTypeTransform customTransform) {
      super(outputSchema, properties, customTransform);
      this.requestedProj = requestedProj;
    }

    @Override
    public ColumnReadProjection readProjection(ColumnMetadata col) {
      RequestedColumn reqCol = requestedProj.get(col.name());
      if (reqCol == null) {
        return new UnprojectedReadColProj(col);
      }
      ColumnMetadata outputCol = outputSchema.metadata(col.name());
      if (outputCol == null) {
        return new ExplicitReadColProj(col, reqCol);
      }
      return schemaReadProj(col, reqCol, outputCol);
    }
  }

  public static class ImplicitProjectionSet implements ProjectionSet {

    public static final ProjectionSet PROJECT_ALL = new ImplicitProjectionSet(true);
    public static final ProjectionSet PROJECT_NONE = new ImplicitProjectionSet(false);

    private final boolean projectAll;

    private ImplicitProjectionSet(Boolean projectAll) {
      this.projectAll = projectAll;
    }

    @Override
    public ColumnReadProjection readProjection(ColumnMetadata col) {
      if (projectAll) {
        return new WildcardReadColProj(col);
      } else {
        return new UnprojectedReadColProj(col);
      }
    }
  }

  public static class ProjectionSetBuilder {

    private Collection<SchemaPath> projectionList;
    private RequestedTuple parsedProjection;
    private TupleMetadata providedSchema;
    private CustomTypeTransform transform;
    private Map<String, String> properties;

    public ProjectionSetBuilder projectionList(Collection<SchemaPath> projection) {
      projectionList = projection;
      return this;
    }

    public ProjectionSetBuilder parsedProjection(RequestedTuple projection) {
      parsedProjection = projection;
      return this;
    }

    public ProjectionSetBuilder outputSchema(TupleMetadata schema) {
      providedSchema = schema;
      return this;
    }

    public ProjectionSetBuilder transform(CustomTypeTransform transform) {
      this.transform = transform;
      return this;
    }

    public ProjectionSet build() {
      // If projection, build the projection map.
      // The caller might have already built the map. If so,
      // use it.

      Preconditions.checkArgument(projectionList == null || parsedProjection == null);
      if (projectionList != null) {
        parsedProjection = RequestedTupleImpl.parse(projectionList);
      }
      TupleProjectionType projType = parsedProjection == null ?
          TupleProjectionType.ALL : parsedProjection.type();
      switch (projType) {
      case ALL:
        if (providedSchema == null) {
          return ProjectionSetFactory.projectAll();
        } else {
          return new WildcardAndSchemaProjectionSet(providedSchema, properties, transform);
        }
      case NONE:
        return ProjectionSetFactory.projectNone();
      case SOME:
        if (providedSchema == null) {
          return new ExplicitProjectionSet(parsedProjection);
        } else {
          return new ExplicitSchemaProjectionSet(parsedProjection, providedSchema, properties, transform);
        }
      default:
        throw new IllegalStateException(projType.toString());
      }
    }
  }
  public static class ProjectionSetFactory {

    public static ProjectionSet projectAll() { return ImplicitProjectionSet.PROJECT_ALL; }

    public static ProjectionSet projectNone() { return ImplicitProjectionSet.PROJECT_NONE; }

    public static ProjectionSet wrap(RequestedTuple mapProjection) {
      switch (mapProjection.type()) {
      case ALL:
        return projectAll();
      case NONE:
        return projectNone();
      case SOME:
        return new ExplicitProjectionSet(mapProjection);
      default:
        throw new IllegalStateException(mapProjection.type().toString());
      }
    }

    public static ProjectionSet build(RequestedTuple projectionSet) {
      // TODO Auto-generated method stub
      return null;
    }
  }
}
