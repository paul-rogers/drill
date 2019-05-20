package org.apache.drill.exec.physical.impl.scan.project;

import org.apache.drill.exec.record.metadata.ColumnMetadata;

public class Exp {

  public interface ProjectionSet {

    /**
     * Determine if the given column is to be projected. Used when
     * adding columns to the result set loader. Skips columns omitted
     * from an explicit projection, or columns within a wildcard projection
     * where the column is "special" and is not expanded in the wildcard.
     */

    boolean isProjected(ColumnMetadata col);

    /**
     * Given a column to be projected (<tt>isProjected()</tt> returned
     * true), validate that the actual type and mode is compatible with
     * the projection.
     * <p>
     * This is subtle: type differences can be handled by type conversion.
     * The check here is for differences that can't be so handled: a
     * projection of type map "a.b", but a column that is an array, say.
     *
     * @throws UserException if the column is incompatible with the
     * projection
     */
    void validateType(ColumnMetadata col);
  }
}
