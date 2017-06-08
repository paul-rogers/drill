package org.apache.drill.exec.physical.impl.scan;

import java.util.HashMap;
import java.util.Map;

import org.apache.drill.exec.physical.impl.scan.ScanProjection.ProjectedColumn;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;

/**
 * Computes the set of columns to select from a table, using
 * table schema order.<br>
 * Table: (t1, t2, t3, t4)<br>
 * Select: (t4, t2)<br>
 * Table selection: (t1, t2*, t3, t4*)</br>
 * Where the asterisk indicates a selected column.
 * <p>
 * The output are four mapping arrays:
 * <ul><li>Boolean selection array of selected columns.</li>
 * <ul><li>Boolean array of matched table columns.</li>
 * <ul><li>Full to partial schema position mapping.</li>
 * <ul><li>Partial to select position mapping.</li>
 */

public class TableSelectionList {

//  public class ProjectionVisitor extends SelectionListPlan.Visitor {
//
//  }

  private static class TableColumn {
    private int index;
    private MaterializedField schema;
    boolean selected;
    int physicalIndex;
  }

  private final SelectionListPlan selection;
  private final BatchSchema tableSchema;
  private Map<String,MaterializedField> tableIndex;
  private boolean selectionMap[];
  private boolean nullColumnMap[];
  private int logicalToPhysicalMap[];
  private int physicalToSelectMap[];
  private int selectToLogicalMap[];

  public TableSelectionList(SelectionListPlan selection, BatchSchema tableSchema) {
    this.selection = selection;
    this.tableSchema = tableSchema;
    switch(selection.selectType()) {
    case COLUMNS_ARRAY:
      break;
    case LIST:
      mapSelectionList();
      break;
    case WILDCARD:
      break;
    default:
      throw new IllegalStateException("Unexpected selection type: " + selection.selectType());
    }

  }

  private boolean[] mapSelectionList() {
    tableIndex = new HashMap<>();
    for (MaterializedField tableCol : tableSchema) {
      tableIndex.put(toKey(tableCol.getName()), tableCol);
    }
    selectionMap = new boolean[tableSchema.getFieldCount()];
    selectToLogicalMap
    for (int i = 0; i < tableSchema.getFieldCount(); i++) {
      MaterializedField tableCol = tableSchema.getColumn(i);
      OutputColumn outCol = selection.column(tableCol.getName());
      flags[i] = outCol != null  &&  outCol.columnType() == ColumnType.TABLE;
    }
  }

  private boolean[] mapNullColumns() {
    // TODO Auto-generated method stub
    return null;
  }

  private int[] mapPhysical() {
    // TODO Auto-generated method stub
    return null;
  }

  private int[] mapSelectToPhysical() {
    // TODO Auto-generated method stub
    return null;
  }
}
