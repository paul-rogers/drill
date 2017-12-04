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
package org.apache.drill.exec.physical.impl.scan.project;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.physical.impl.scan.project.NullColumnLoader.NullColumnSpec;
import org.apache.drill.exec.physical.impl.scan.project.RowBatchMerger.Projection;
import org.apache.drill.exec.physical.impl.scan.project.RowBatchMerger.VectorSource;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ColumnProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.UnresolvedColumn;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.TupleMetadata;

/**
 * Computes the full output schema given a table (or batch)
 * schema. Takes the original, unresolved output list from the projection
 * definition, merges it with the file, directory and table schema information,
 * and produces a partially or fully resolved output list.
 * <p>
 * A "resolved" projection list is a list of concrete columns: table
 * columns, nulls, file metadata or partition metadata. An unresolved list
 * has either table column names, but no match, or a wildcard column.
 * <p>
 * The idea is that the projection list moves through stages of resolution
 * depending on which information is available. An "early schema" table
 * provides schema information up front, and so allows fully resolving
 * the projection list on table open. A "late schema" table allows only a
 * partially resolved projection list, with the remainder of resolution
 * happening on the first (or perhaps every) batch.
 * <p>
 * Data source (table) schema can be of two forms:
 * <ul>
 * <li>Early schema: the schema is known before reading data. A JDBC data
 * source is an example, as is a CSV reader for a file with headers.</li>
 * <li>Late schema: the schema is not known until data is read, and is
 * discovered on the fly. Example: JSON, which declares values as maps
 * without an up-front schema.</li>
 * </ul>
 * These two forms give rise to distinct ways of planning the projection.
 * <p>
 * The final result of the projection is a set of "output" columns: a set
 * of columns that, taken together, defines the row (bundle of vectors) that
 * the scan operator produces. Columns are ordered: the order specified here
 * must match the order that columns appear in the result set loader and the
 * vector container so that code can access columns by index as well as name.
 *
 * @see {@link ImplicitColumnExplorer}, the class from which this class
 * evolved
 */

public class SchemaLevelProjection {

  /**
   * Schema-level projection is customizable. Implement this interface, and
   * add an instance to the scan orchestrator, to perform custom mappings
   * from unresolved columns (perhaps of an extension-specified type) to
   * final projected columns. The metadata manager, for example, implements
   * this interface to map metadata columns.
   */

  public interface SchemaProjectionResolver {
    void bind(SchemaLevelProjection projection);
    boolean resolveColumn(ColumnProjection col);
  }

  /**
   * A resolved column has a name, and a specification for how to project
   * data from a source vector to a vector in the final output container.
   */

  public interface ResolvedColumn extends ColumnProjection {
    Projection projection();
    MaterializedField schema();
  }

  /**
   * Column that matches one provided by the table. Provides the data type
   * of that column and information to project from the result set loader
   * output container to the scan output container. (Note that the result
   * set loader container is, itself, a projection from the actual table
   * schema to the desired set of columns; but in the order specified
   * by the table.)
   */

  public static class ResolvedTableColumn implements ResolvedColumn {

    public static final int ID = 3;

    public final String projectedName;
    public final MaterializedField schema;
    public final Projection projection;

    public ResolvedTableColumn(String projectedName,
        MaterializedField schema,
        Projection projection) {
      this.projectedName = projectedName;
      this.schema = schema;
      this.projection = projection;
    }

    @Override
    public String name() { return projectedName; }

    @Override
    public Projection projection() { return projection; }

    @Override
    public MaterializedField schema() { return schema; }

    @Override
    public int nodeType() { return ID; }

    @Override
    public boolean isTableProjection() { return true; }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder();
      buf
        .append("[")
        .append(getClass().getSimpleName())
        .append(" name=")
        .append(name())
        .append(", projection=")
        .append(projection == null ? "null" : projection.toString())
        .append("]");
      return buf.toString();
    }
  }

  /**
   * Perform a wildcard projection. In this case, the query wants all
   * columns in the source table, so the table drives the final projection.
   * Since we include only those columns in the table, there is no need
   * to create null columns. Example: SELECT *
   */

  public static class WildcardSchemaProjection extends SchemaLevelProjection {

    public WildcardSchemaProjection(ScanLevelProjection scanProj,
        TupleMetadata tableSchema,
        VectorSource tableSource,
        List<SchemaProjectionResolver> resolvers) {
      super(tableSchema, tableSource, resolvers);
      for (ColumnProjection col : scanProj.columns()) {
        if (col.nodeType() == UnresolvedColumn.WILDCARD) {
          for (int i = 0; i < tableSchema.size(); i++) {
            MaterializedField colSchema = tableSchema.column(i);
            resolveTableColumn(colSchema.getName(), colSchema, i);
          }
        } else {
          resolveSpecial(col);
        }
      }
    }
  }

  /**
   * Projected column that serves as both a resolved column (provides projection
   * mapping) and a null column spec (provides the information needed to create
   * the required null vectors.)
   */

  public static class NullProjectedColumn implements ResolvedColumn, NullColumnSpec {

    public static final int ID = 4;

    private final String name;
    private final MajorType type;
    private final Projection projection;

    public NullProjectedColumn(String name, MajorType type, Projection projection) {
      this.name = name;
      this.type = type;
      this.projection = projection;
     }

    @Override
    public MajorType type() { return type; }

    @Override
    public String name() { return name; }

    @Override
    public Projection projection() { return projection; }

    @Override
    public MaterializedField schema() {
      return MaterializedField.create(name, type);
    }

    @Override
    public int nodeType() { return ID; }

    @Override
    public boolean isTableProjection() { return false; }
  }

  /**
   * Perform a schema projection for the case of an explicit list of
   * projected columns. Example: SELECT a, b, c.
   * <p>
   * An explicit projection starts with the requested set of columns,
   * then looks in the table schema to find matches. That is, it is
   * driven by the query itself.
   * <p>
   * An explicit projection may include columns that do not exist in
   * the source schema. In this case, we fill in null columns for
   * unmatched projections.
   */

  public static class ExplicitSchemaProjection extends SchemaLevelProjection {

    protected List<NullColumnSpec> nullCols = new ArrayList<>();
    protected VectorSource nullSource;

    public ExplicitSchemaProjection(ScanLevelProjection scanProj,
        TupleMetadata tableSchema,
        VectorSource tableSource,
        VectorSource nullSource,
        List<SchemaProjectionResolver> resolvers) {
      super(tableSchema, tableSource, resolvers);
      this.nullSource = nullSource;

      for (ColumnProjection col : scanProj.columns()) {
        if (col.nodeType() == UnresolvedColumn.UNRESOLVED) {
          resolveColumn(col);
        } else {
          resolveSpecial(col);
        }
      }
    }

    private void resolveColumn(ColumnProjection col) {
      int tableColIndex = tableSchema.index(col.name());
      if (tableColIndex == -1) {
        resolveNullColumn(col);
      } else {
        resolveTableColumn(col.name(), tableSchema.column(tableColIndex), tableColIndex);
      }
    }

    private void resolveNullColumn(ColumnProjection col) {
      NullProjectedColumn nullCol = new NullProjectedColumn(col.name(), null,
          new Projection(nullSource, true, nullCols.size(), outputIndex()));
      addOutputColumn(nullCol);
      nullCols.add(nullCol);
    }

    @Override
    public List<NullColumnSpec> nullColumns() { return nullCols; }
  }

  protected TupleMetadata tableSchema;
  protected VectorSource tableSource;
  protected List<ResolvedColumn> output = new ArrayList<>();
  protected List<SchemaProjectionResolver> resolvers;

  protected SchemaLevelProjection(
        TupleMetadata tableSchema,
        VectorSource tableSource,
        List<SchemaProjectionResolver> resolvers) {
    if (resolvers == null) {
      resolvers = new ArrayList<>();
    }
    this.tableSchema = tableSchema;
    this.tableSource = tableSource;
    this.resolvers = resolvers;
    for (SchemaProjectionResolver resolver : resolvers) {
      resolver.bind(this);
    }
  }

  protected void resolveTableColumn(String colName, MaterializedField col, int tableColIndex) {
    addOutputColumn(new ResolvedTableColumn(colName, col,
        tableProjection(tableColIndex)));
  }

  public void addOutputColumn(ResolvedColumn col) {
    output.add(col);
  }

  public Projection tableProjection(int tableColIndex) {
    return new Projection(tableSource, true, tableColIndex, outputIndex());
  }

  protected void resolveSpecial(ColumnProjection col) {
    for (SchemaProjectionResolver resolver : resolvers) {
      if (resolver.resolveColumn(col)) {
        return;
      }
    }
    throw new IllegalStateException("No resolver for column: " + col.nodeType());
  }

  public int outputIndex() { return output.size(); }
  public TupleMetadata tableSchema() { return tableSchema; }
  public List<ResolvedColumn> columns() { return output; }
  public List<NullColumnSpec> nullColumns() { return null; }
}
