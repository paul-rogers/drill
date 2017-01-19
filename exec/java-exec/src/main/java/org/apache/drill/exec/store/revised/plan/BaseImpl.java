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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.revised.Sketch;
import org.apache.drill.exec.store.revised.Sketch.FilterExpr;
import org.apache.drill.exec.store.revised.Sketch.LogicalSchema;
import org.apache.drill.exec.store.revised.Sketch.LogicalTable;
import org.apache.drill.exec.store.revised.Sketch.QualifiedName;
import org.apache.drill.exec.store.revised.Sketch.RowSchema;
import org.apache.drill.exec.store.revised.Sketch.ScanBuilder;
import org.apache.drill.exec.store.revised.Sketch.ScanSelector;
import org.apache.drill.exec.store.revised.Sketch.SchemaResolver;
import org.apache.drill.exec.store.revised.Sketch.TableInterator;
import org.apache.drill.exec.store.revised.Sketch.TableResolver;
import org.apache.drill.exec.store.revised.Sketch.TableScan;
import org.apache.drill.exec.store.revised.Sketch.TableScanCreator;
import org.apache.drill.exec.store.revised.plan.ExtensionBuilder.LogicalSchemaBuilder;
import org.apache.drill.exec.store.revised.retired.StorageExtension;
import org.apache.drill.exec.store.revised.retired.StoragePluginAdapter;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

public class BaseImpl {

  public abstract static class BaseExtensionPlugin<T extends StorageExtension, C extends StoragePluginConfig> extends StoragePluginAdapter<T, C> {

    protected BaseExtensionPlugin(C config,
        DrillbitContext context, String schemaName, T extension) {
      super(config, context, schemaName, extension);
    }

    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent)
        throws IOException {
      @SuppressWarnings("resource")
      AbstractSchema drillSchema = buildRootSchema();
      parent.add(drillSchema.getName(), drillSchema);
    }

    private AbstractSchema buildRootSchema() {
      return new SchemaAdapter(this, extension().rootSchema());
    }

    @Override
    public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection, List<SchemaPath> columns)
        throws IOException {

      String tableName = selection.getListWith(new ObjectMapper(),
          new TypeReference<String>() {
          });

      LogicalSchema schema = extension().rootSchema();
      LogicalTable table = schema.table(tableName);
      if (table == null) {
        throw new IllegalStateException("Table can no longer be found: " + table);
      }
      TableScan scan = schema.scan(table);
      scan.userName(userName);
      scan.columns(columns);
      return new GroupScanAdapter(scan);
    }

    public boolean supportsProject() {
      return false;
    }
  }

  public static class GroupScanAdapter extends AbstractGroupScan {

    private TableScan tableScan;

    public GroupScanAdapter(TableScan tableScan) {
      super(tableScan.userName());
      this.tableScan = tableScan;
    }

    @Override
    public void applyAssignments(List<DrillbitEndpoint> endpoints)
        throws PhysicalOperatorSetupException {
      tableScan.setAssignments(endpoints);
      tableScan.buildPhysicalScans();
    }

    @Override
    public ScanStats getScanStats() {
      // TODO: Move this to table scan.
      return ScanStats.TRIVIAL_TABLE;
    }

    @Override
    public SubScan getSpecificScan(int minorFragmentId)
        throws ExecutionSetupException {
      return tableScan.getPhysicalScan(minorFragmentId);
    }

    @Override
    public int getMaxParallelizationWidth() {
      return tableScan.partitionCount();
    }

    @Override
    public String getDigest() {
      return tableScan.toString();
    }

    @Override
    public boolean canPushdownProjects(List<SchemaPath> columns) {
      if (tableScan.supportsProject()) {
        tableScan.columns(columns);
        return true;
      }
      return false;
    }

    // Clone makes a copy of the node, but (strangely) also provides the
    // list of columns if this scan supports project push-down.

    @Override
    public GroupScan clone(List<SchemaPath> columns) {
      tableScan.columns(columns);
      return new GroupScanAdapter(tableScan);
    }

    // Here we assume that the node will be copied, but that the
    // planner will ever only with with the one plan.
    // Said another way, the planner, as it proceeds, will produce
    // many trees in which this group scan nodes is a leaf. However
    // only one of those plans will be carried through to produce
    // physical scans. This allows us to use a single TableScan
    // shared across the many group scan nodes.

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
        throws ExecutionSetupException {
      assert children.isEmpty();
      return new GroupScanAdapter(tableScan);
    }
  }

  public static class SchemaAdapter extends AbstractSchema {

    private LogicalSchema logicalSchema;
    private StoragePluginAdapter<?,?> plugin;

    public SchemaAdapter(StoragePluginAdapter<?,?> plugin, LogicalSchema logicalSchema) {
      super(logicalSchema.fullName().parentName(), logicalSchema.schemaName());
      this.plugin = plugin;
      this.logicalSchema = logicalSchema;
    }

    @Override
    public Table getTable(String tableName) {
      LogicalTable logicalTable = logicalSchema.table(tableName);
      if (logicalTable == null) {
        return null; }
      return new DynamicDrillTable(plugin, name, tableName);
    }

    @Override
    public String getTypeName() {
      return plugin.name();
    }

    @Override
    public Set<String> getTableNames() {
      Set<String> names = new HashSet<>( );
      Iterable<LogicalTable> tables = logicalSchema.tables();
      if (tables != null) {
        for (LogicalTable t : tables) {
          names.add(t.name());
        }
      }
      return names;
    }

  }

  public static class BaseExtension<C extends StoragePluginConfig> implements StorageExtension {

    private final C config;
    private final LogicalSchema rootSchema;

    public BaseExtension(ExtensionBuilder<C> builder) {
      rootSchema = builder.rootSchema;
      config = builder.config;
      rootSchema.bind(this);
    }

    @Override
    public LogicalSchema rootSchema() {
      return rootSchema;
    }

    public C config() {
      return config;
    }
  }

  public static class AbstractLogicalSchema implements LogicalSchema {

    private StorageExtension extension;
    private final LogicalSchema parent;
    private final QualifiedName name;
    private final TableInterator tableIterator;
    private final TableResolver tableResolver;
    private final SchemaResolver schemaResolver;
    private final TableScanCreator tableScanCreator;
    private final ScanSelector scanSelector;

    public AbstractLogicalSchema(LogicalSchemaBuilder builder) {
      parent = builder.parent;
      name = builder.schemaName;
      tableIterator = builder.tableIterator;
      tableResolver = builder.tableResolver;
      schemaResolver = builder.schemaResolver;
      tableScanCreator = builder.tableScanCreator;
      scanSelector = builder.scanSelector;
    }

    @Override
    public void bind(final StorageExtension extension) {
      this.extension = extension;
    }

    @Override
    public StorageExtension extension() { return extension; }

    @Override
    public String schemaName() {
      return name.tail();
    }

    @Override
    public LogicalSchema resolveSchema(String name) {
      if (schemaResolver == null) {
        return null; }
      return schemaResolver.resolve(this, name);
    }

    @Override
    public LogicalSchema parent() {
      return parent;
    }

    @Override
    public QualifiedName fullName() {
      return name;
    }

    @Override
    public Iterable<LogicalTable> tables() {
      if (tableIterator == null) {
        return null;
      }
      return tableIterator.tables(this);
    }

    @Override
    public LogicalTable table(String name) {
      if (tableResolver == null) {
        return null;
      }
      return tableResolver.resolve(this, name);
    }

    @Override
    public TableScan scan(LogicalTable table) {
      if (tableScanCreator == null) {
        throw new IllegalStateException( "No table scan creator defined for " + name.fullName() );
      }
      if (scanSelector == null) {
        throw new IllegalStateException( "No table scan selector defined for " + name.fullName() );
      }
      TableScan tableScan = tableScanCreator.scan(table);
      ScanBuilder scanBuilder = scanSelector.select(table);
      if (scanBuilder == null) {
        throw new IllegalStateException( "No table scan defined for " + name.fullName( ) + "." + table.name() );
      }
      tableScan.scanBuilder(scanBuilder);
      return tableScan;
    }
  }

//  public abstract static class AbstractSchemaReader implements SchemaReader {
//
//    @Override
//    public Iterable<LogicalTable> tables() {
//      return null;
//    }
//
//    @Override
//    public LogicalTable table(String name) {
//      return new AbstractLogicalTable(name);
//    }
//  }

  public static class AbstractLogicalTable implements LogicalTable {

    private final LogicalSchema schema;
    private final String name;

    public AbstractLogicalTable(LogicalSchema schema, String name) {
      this.schema = schema;
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
      return schema;
    }

    @Override
    public int partitionCount() {
      return 1;
    }
  }

  public static class AbstractTableScan implements TableScan {

    private LogicalTable table;
    protected int partitionCount = -1;
    protected String userName;
    protected List<SchemaPath> columns;
    private List<DrillbitEndpoint> endpoints;
    private ScanBuilder scanBuilder;
    private List<SubScan> physicalScans;

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

    @Override
    public void userName(String userName) {
      this.userName = userName;
    }

    @Override
    public String userName() {
      return userName;
    }

    @Override
    public void columns(List<SchemaPath> columns) {
      this.columns = columns;
    }

    public List<SchemaPath> columns() { return columns; }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder( )
          .append("[")
          .append(getClass().getSimpleName())
          .append(": Table=")
          .append(table.name());
      if (userName != null) {
        buf.append(", User=")
          .append(userName);
      }
      if (columns != null) {
        buf.append(", Columns=")
          .append(columns.toString());
      }
      buf.append("]");
      return buf.toString();
    }

    @Override
    public int partitionCount() {
      if (partitionCount != -1) {
        return partitionCount;
      }
      return table.partitionCount();
    }

    @Override
    public void setAssignments(List<DrillbitEndpoint> endpoints) {
      this.endpoints = endpoints;
    }

    public List<DrillbitEndpoint> endpoints() { return endpoints; }

    @Override
    public boolean supportsProject() {
      return scanBuilder.supportsProject();
    }

    @Override
    public void buildPhysicalScans() {
      physicalScans = scanBuilder.build(this);
    }

    @Override
    public SubScan getPhysicalScan(int minorFragmentIndex) {
      return physicalScans.get(minorFragmentIndex);
    }

    @Override
    public void scanBuilder(ScanBuilder scanBuilder) {
      this.scanBuilder = scanBuilder;
    }

    @Override
    public LogicalTable table() {
      return table;
    }
  }

  public static class QualifiedNameImpl implements QualifiedName {

    private final List<String> names;

    public QualifiedNameImpl(String path) {
      names = Splitter.on('.').splitToList(path);
    }

    public QualifiedNameImpl(QualifiedName base, QualifiedName tail) {
      names = new ArrayList<>( );
      names.addAll(base.parts());
      names.addAll(tail.parts());
    }

    public QualifiedNameImpl(QualifiedName base, String tail) {
      names = new ArrayList<>( );
      names.addAll(base.parts());
      names.add(tail);
    }

    @Override
    public String tail() {
      return names.get(names.size()-1);
    }

    @Override
    public String fullName() {
      return Joiner.on('.').join(names);
    }

    @Override
    public List<String> parts() {
      return names;
    }

    @Override
    public List<String> parentName() {
      if (names.size() < 2) {
        return new ArrayList<String>();
      }
      return names.subList(0, names.size() - 2);
    }

    @Override
    public String toString() {
      return fullName();
    }

  }
}
