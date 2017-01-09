package org.apache.drill.exec.store.revised;

import org.apache.drill.exec.store.revised.Sketch.*;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.store.revised.BaseImpl.*;

public class ExtensionBuilder<C extends StoragePluginConfig> {

  public static class LogicalSchemaBuilder {

    private QualifiedName schemaName;
    private TableInterator tableIterator;
    private TableResolver tableResolver;
    private SchemaResolver schemaResolver;
    private TableScanCreator tableScanCreator;
    private LogicalSchema parent;

    public LogicalSchemaBuilder(String schemaName) {
      this.schemaName = new QualifiedNameImpl(schemaName);
    }

    public LogicalSchemaBuilder parent(LogicalSchema parent) {
      this.parent = parent;
      return this;
    }

    public LogicalSchemaBuilder tableIterator(TableInterator tableIterator) {
      this.tableIterator = tableIterator;
      return this;
    }

    public LogicalSchemaBuilder tableResolver(TableResolver tableResolver) {
      this.tableResolver = tableResolver;
      return this;
    }

    public LogicalSchemaBuilder trivialTableResolver() {
      this.tableResolver = new TableResolver() {

        @Override
        public LogicalTable resolve(LogicalSchema schema, String name) {
          return new AbstractLogicalTable(schema, name);
        }
      };
      this.tableScanCreator = new TableScanCreator() {

        @Override
        public TableScan scan(LogicalTable table) {
          return new AbstractTableScan(table);
        }

      };
      return this;
    }

    public LogicalSchemaBuilder schemaResolver(SchemaResolver schemaResolver) {
      this.schemaResolver = schemaResolver;
      return this;
    }

    public LogicalSchemaBuilder tableScanCreator(TableScanCreator tableScanCreator) {
      this.tableScanCreator = tableScanCreator;
      return this;
    }

    public LogicalSchema buildSchema(LogicalSchema parent, QualifiedName schemaName) {
      if (tableResolver != null && tableScanCreator == null) {
        throw new IllegalArgumentException("If a table resolver is provided, must also provide a scan creator.");
      }
      AbstractLogicalSchema schema = new AbstractLogicalSchema(parent, schemaName);
      schema.setTableIterator(tableIterator);
      schema.setTableResolver(tableResolver);
      schema.setSchemaResolver(schemaResolver);
      schema.setTableScanCreator(tableScanCreator);
      return schema;
    }

    public LogicalSchema build() {
      return buildSchema(parent, schemaName);
    }
  }

  private C config;
  private LogicalSchema rootSchema;

  public ExtensionBuilder(LogicalSchema rootSchema, C config) {
    this.rootSchema = rootSchema;
    this.config = config;
  }

  public StorageExtension build( ) {
    return new BaseExtension<C>(rootSchema, config);
  }

}
