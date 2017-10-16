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
import org.apache.drill.exec.record.TupleMetadata;

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

public class TableLevelProjection {

  public interface ResolvedColumn {
    String name();
    Projection projection();
  }

  public interface TableProjectionResolver {
    ResolvedColumn resolve(ColumnProjection col);
  }

  public static class ResolvedTableColumn implements ResolvedColumn {

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
  }

  public static class WildcardTableProjection extends TableLevelProjection {

    public WildcardTableProjection(ReaderLevelProjection fileProj,
        TupleMetadata tableSchema,
        VectorSource tableSource) {
      super(tableSchema, tableSource);
      for (ColumnProjection col : fileProj.output()) {
        if (col.nodeType() == UnresolvedColumn.WILDCARD) {
          for (int i = 0; i < tableSchema.size(); i++) {
            MaterializedField colSchema = tableSchema.column(i);
            output.add(new ResolvedTableColumn(colSchema.getName(),
                colSchema,
                new Projection(tableSource, false, i, outputIndex())));
          }
        } else {
          resolveSpecial(col);
        }
      }
    }
  }

  public static class NullColumn implements ResolvedColumn, NullColumnSpec {

    private final String name;
    private final MajorType type;
    private final Projection projection;

    public NullColumn(String name, MajorType type, Projection projection) {
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
  }

  public static class ExplicitTableProjection extends TableLevelProjection {
    protected List<NullColumnSpec> nullCols = new ArrayList<>();
    protected VectorSource nullSource;

    public ExplicitTableProjection(ReaderLevelProjection fileProj,
        TupleMetadata tableSchema,
        VectorSource tableSource,
        VectorSource nullSource) {
      super(tableSchema, tableSource);
      this.nullSource = nullSource;
      for (ColumnProjection col : fileProj.output()) {
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
         output.add(new ResolvedTableColumn(col.name(), tableSchema.column(tableColIndex),
             new Projection(tableSource, false, tableColIndex, outputIndex())));
      }
    }

    private void resolveNullColumn(ColumnProjection col) {
      MajorType colType;
      if (col.nodeType() == ContinuedColumn.ID) {
        colType = ((ContinuedColumn) col).type();
      } else {
        colType = null;
      }
      NullColumn nullCol = new NullColumn(col.name(), colType,
          new Projection(nullSource, true, nullCols.size(), outputIndex()));
      output.add(nullCol);
      nullCols.add(nullCol);
    }
  }

  protected TupleMetadata tableSchema;
  protected VectorSource tableSource;
  protected List<ResolvedColumn> output = new ArrayList<>();
  protected List<TableProjectionResolver> resolvers;

  protected TableLevelProjection(
        TupleMetadata tableSchema,
        VectorSource tableSource) {
    this.tableSchema = tableSchema;
    this.tableSource = tableSource;
  }

  protected void resolveSpecial(ColumnProjection col) {
    for (TableProjectionResolver resolver : resolvers) {
      ResolvedColumn resolved = resolver.resolve(col);
      if (resolved != null) {
        output.add(resolved);
      }
    }
    throw new IllegalStateException("No resolver for column: " + col.nodeType());
  }

  protected int outputIndex() { return output.size(); }

  public List<ResolvedColumn> columns() { return output; }
}
