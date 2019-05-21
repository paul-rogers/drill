package org.apache.drill.exec.physical.impl.scan.project;

import org.apache.drill.exec.physical.rowSet.project.RequestedTuple;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.convert.ColumnConversionFactory;
import org.apache.drill.exec.vector.accessor.convert.StandardConversions.ConversionDefn;

/**
 * A key purpose of these classes is to filters added dynamically
 * at scan time. The reader may offer a column (as to add a column
 * writer for the column.) The projection mechanism says whether to
 * materialize the column, or whether to ignore the column and
 * return a dummy column writer.
 * <p>
 * The Project All must handle several additional nuances:
 * <ul>
 * <li>External schema: If an external schema is provided, then that
 * schema may be "strict" which causes the wildcard to expand to the
 * set of columns defined within the schema. When used with columns
 * added dynamically, a column may be excluded from the projection
 * set if it is not part of the defined external schema.</ul>
 * <li>Metadata filtering: A reader may offer a special column which
 * is available only in explicit projection, and behaves like Drill's
 * implicit file columns. Such columns are not included in a "project
 * all" projection.</li>
 * <p>
 * At present, only the top-level row supports these additional filtering
 * options; they are not supported on mays (though could be with additional
 * effort.)
 * <p>
 * Special columns are generic and thus handled here. External schema
 * is handled in a subclass in the scan projection framework.
 * <p>
 */
public interface ProjectionSet {

  /**
   * Response to a query against a reader projection to indicate projection
   * status of a reader-provided column. This is a transient object which
   * indicates whether a reader column is projected, and if so, the attributes
   * of that projection.
   */

  public interface ColumnReadProjection {

    /**
     * Determine if the given column is to be projected. Used when
     * adding columns to the result set loader. Skips columns omitted
     * from an explicit projection, or columns within a wildcard projection
     * where the column is "special" and is not expanded in the wildcard.
     */

    boolean isProjected();

    ColumnMetadata inputSchema();
    ColumnMetadata outputSchema();

    ColumnConversionFactory conversionFactory();
    RequestedTuple mapProjection();
  }

  public interface CustomTypeTransform {
    ColumnConversionFactory transform(ColumnMetadata inputDefn,
        ColumnMetadata outputDefn, ConversionDefn defn);
  }

  ColumnReadProjection readProjection(ColumnMetadata col);
}
