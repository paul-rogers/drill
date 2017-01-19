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
package org.apache.drill.exec.store.revised.plan;

import org.apache.drill.exec.store.revised.Sketch;
import org.apache.drill.exec.store.revised.Sketch.*;
import org.apache.drill.exec.store.revised.plan.BaseImpl.*;
import org.apache.drill.exec.store.revised.retired.StorageExtension;
import org.apache.drill.common.logical.StoragePluginConfig;

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
