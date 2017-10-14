package org.apache.drill.exec.physical.impl.scan.project;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.physical.impl.scan.project.ColumnProjection.TypedColumn;
import org.apache.drill.exec.record.MaterializedField;

/**
 * Represents a table column projected to the output.
 */

public class ProjectedColumn extends TypedColumn {

  private final int columnIndex;

  public ProjectedColumn(MaterializedField schema, int columnIndex) {
    super(schema, TypedColumn.PROJECTED, true);
    this.columnIndex = columnIndex;
  }

  public ProjectedColumn(String name, MajorType type, int columnIndex) {
    super(name, type, TypedColumn.PROJECTED, true);
    this.columnIndex = columnIndex;
  }

//  public static ProjectedColumn fromWildcard(WildcardColumn col,
//      int columnIndex, MaterializedField column) {
//    return new ProjectedColumn(col.source(), column.getName(),
//        columnIndex, column);
//  }
//
//  public static ProjectedColumn fromResolution(RequestedTableColumn col,
//      int columnIndex, MaterializedField column) {
//    return new ProjectedColumn(col.source(), col.name(),
//        columnIndex, column);
//  }
//
//  public static ProjectedColumn fromColumnsArray(ColumnsArrayColumn col) {
//    return new ProjectedColumn(col.source(), col.inCol.rootName(), 0,
//        MaterializedField.create(col.inCol.rootName(), col.type()));
//  }

  public int columnIndex() { return columnIndex; }

//  @Override
//  public int nodeType() { return ID; }

  @Override
  public ColumnProjection unresolve() {
    return TypedColumn.newContinued(schema());
  }

//  @Override
//  protected void buildString(StringBuilder buf) {
//    buf.append(", index=")
//       .append(columnIndex);
//  }
//
//  @Override
//  protected void visit(int index, Visitor visitor) {
//    visitor.visitProjection(index, this);
//  }
//
//  @Override
//  public RequestedTableColumn unresolve() {
//    return RequestedTableColumn.fromUnresolution(this);
//  }

  @Override
  public String toString() {
    return new StringBuilder()
        .append("[")
        .append(getClass().getSimpleName())
        .append(schema().toString())
        .append(", index=")
        .append(columnIndex)
        .append(", id=")
        .append(nodeType())
        .append("]")
        .toString();
  }
}