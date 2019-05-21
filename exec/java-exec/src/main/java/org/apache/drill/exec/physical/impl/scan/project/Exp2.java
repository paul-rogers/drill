package org.apache.drill.exec.physical.impl.scan.project;

import org.apache.drill.exec.physical.impl.scan.project.Exp.BaseReadColProj;
import org.apache.drill.exec.physical.impl.scan.project.ProjectionSet.ColumnReadProjection;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple.RequestedColumn;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.convert.ColumnConversionFactory;

public class Exp2 {

  public static abstract class AbstractReadColProj implements ColumnReadProjection {
    protected final ColumnMetadata readSchema;

    public AbstractReadColProj(ColumnMetadata readSchema) {
      this.readSchema = readSchema;
    }

    @Override
    public ColumnMetadata inputSchema() { return readSchema; }

    @Override
    public boolean isProjected() { return true; }

    @Override
    public ColumnConversionFactory conversionFactory() { return null; }

    @Override
    public ColumnMetadata outputSchema() { return null; }

    @Override
    public RequestedTuple mapProjection() { return null; }
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

  public static abstract class AbstractReadColProj2 implements ColumnReadProjection {
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

}
