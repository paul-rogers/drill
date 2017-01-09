package org.apache.drill.exec.store.revised;

import java.util.List;

import org.apache.drill.exec.store.revised.Sketch.*;

public class BaseImpl {

  public static class BaseExtension implements StorageExtension {
    private final LogicalSchema rootSchema;

    public BaseExtension(LogicalSchema rootSchema) {
      this.rootSchema = rootSchema;
    }

    @Override
    public LogicalSchema rootSchema() {
      return rootSchema;
    }
  }

  public static class AbstractLogicalSchema implements LogicalSchema {

    private String name;
    private SchemaReader reader;
    private SchemaWriter writer;

    public AbstractLogicalSchema(String schemaName, SchemaReader reader, SchemaWriter writer) {
      name = schemaName;
      this.reader = reader;
      this.writer = writer;
    }

    @Override
    public String schemaName() {
      return name;
    }

    @Override
    public StorageSpace storage() {
      return null;
    }

    @Override
    public SchemaReader reader() {
      return reader;
    }

    @Override
    public SchemaWriter writer() {
      return writer;
    }

    @Override
    public LogicalSchema resolveSchema(String name) {
      return null;
    }

    @Override
    public LogicalSchema parent() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public SchemaPath fullName() {
      // TODO Auto-generated method stub
      return null;
    }
  }

  public abstract static class AbstractSchemaReader implements SchemaReader {

    @Override
    public Iterable<LogicalTable> tables() {
      return null;
    }

    @Override
    public LogicalTable table(String name) {
      return new AbstractLogicalTable(name);
    }
  }

  public static class AbstractLogicalTable implements LogicalTable {

    private final String name;

    public AbstractLogicalTable(String name) {
      this.name = name;
    }

    @Override
    public String name() {
       return name;
    }

    @Override
    public int capabilites() {
      return LogicalTable.READ;
    }

    @Override
    public boolean staticSchema() {
      return false;
    }

    @Override
    public RowSchema rowSchema() {
      return null;
    }

    @Override
    public LogicalSchema nameSpace() {
      // TODO Auto-generated method stub
      return null;
    }
  }


  public static class AbstractTableScan implements TableScan {

    private LogicalTable table;
    protected int partitionCount = 1;

    public AbstractTableScan(LogicalTable table) {
      this.table = table;
    }

    @Override
    public List<String> select(List<String> cols) {
      return cols;
    }

    @Override
    public List<FilterExpr> where(List<FilterExpr> exprs) {
      return exprs;
    }

  }
}
