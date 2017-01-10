package org.apache.drill.exec.store.revised;

import org.apache.drill.exec.store.revised.Sketch.*;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.store.revised.BaseImpl.*;

public class ExtensionBuilder<C extends StoragePluginConfig> {


  public static class TrivialScanSelector implements ScanSelector {

    private ScanBuilder scanBuilder;

    public TrivialScanSelector(ScanBuilder scanBuilder) {
      this.scanBuilder = scanBuilder;
    }

    @Override
    public ScanBuilder select(LogicalTable table) {
      return scanBuilder;
    }
  }
  public static class LogicalSchemaBuilder {

    protected QualifiedName schemaName;
    protected TableInterator tableIterator;
    protected TableResolver tableResolver;
    protected SchemaResolver schemaResolver;
    protected TableScanCreator tableScanCreator;
    protected LogicalSchema parent;
    protected ScanSelector scanSelector;
    protected ScanBuilder scanBuilder;

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

    public LogicalSchemaBuilder scanBuilder(ScanBuilder scanBuilder) {
      this.scanBuilder = scanBuilder;
      return this;
    }

    public LogicalSchemaBuilder scanSelector(ScanSelector scanSelector) {
      this.scanSelector = scanSelector;
      return this;
    }

    public LogicalSchema build() {
      if (scanSelector != null && scanBuilder != null) {
        throw new IllegalArgumentException("Provide either a scan selector or builder, but not both.");
      }
      if (scanBuilder != null) {
        scanSelector = new TrivialScanSelector(scanBuilder);
      }
      if (scanSelector != null && tableScanCreator == null) {
        tableScanCreator = new TableScanCreator() {

          @Override
          public TableScan scan(LogicalTable table) {
            return new AbstractTableScan(table);
          }
        };
      }
      if (tableResolver != null && tableScanCreator == null) {
        throw new IllegalArgumentException("If a table resolver is provided, must also provide a scan creator.");
      }

      return new AbstractLogicalSchema(this);
    }
  }

  protected C config;
  protected LogicalSchema rootSchema;

  public ExtensionBuilder(LogicalSchema rootSchema, C config) {
    this.rootSchema = rootSchema;
    this.config = config;
  }

  public StorageExtension build( ) {
    return new BaseExtension<C>(this);
  }

}
