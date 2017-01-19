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
package org.apache.drill.exec.store.revised.proto;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.AbstractSubScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.revised.Sketch.LogicalSchema;
import org.apache.drill.exec.store.revised.Sketch.LogicalTable;
import org.apache.drill.exec.store.revised.Sketch.ScanBuilder;
import org.apache.drill.exec.store.revised.Sketch.TableScan;
import org.apache.drill.exec.store.revised.Sketch.TableScanCreator;
import org.apache.drill.exec.store.revised.plan.ExtensionBuilder;
import org.apache.drill.exec.store.revised.plan.BaseImpl.BaseExtensionPlugin;
import org.apache.drill.exec.store.revised.plan.ExtensionBuilder.LogicalSchemaBuilder;
import org.apache.drill.exec.store.revised.retired.StorageExtension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

public class ProtoPlugin extends BaseExtensionPlugin<StorageExtension, ProtoPluginConfig> {

  public ProtoPlugin(ProtoPluginConfig configuration,
      DrillbitContext context, String name) {
    super(configuration, context, name,
        buildExtension(name, configuration));
  }

  private static StorageExtension buildExtension(String name,
      ProtoPluginConfig configuration) {
    LogicalSchema root = new LogicalSchemaBuilder(name)
        .trivialTableResolver()
        .scanBuilder(new ScanBuilder() {

          @Override
          public boolean supportsProject() { return true; }

          @Override
          public List<SubScan> build(TableScan scan) {
            List<SubScan> scans = new ArrayList<>();
            scans.add(new ProtoSubScanPop(scan.table().name()));
            return scans;
          }
         })
        .build();
    return new ExtensionBuilder<ProtoPluginConfig>(root, configuration)
        .build();
  }

//  public class ProtoSchema extends AbstractSchema {
//
//    public ProtoSchema() {
//      super(ImmutableList.<String>of(), "proto");
//    }
//
//    @Override
//    public Table getTable(String tableName) {
//      return new DynamicDrillTable(ProtoPlugin.this, name, tableName);
//    }
//
//    @Override
//    public String getTypeName() {
//      return ProtoPluginConfig.NAME;
//    }
//
//    @Override
//    public Set<String> getTableNames() {
//      return new HashSet<>( );
//    }
//
//  }

//  @Override
//  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent)
//      throws IOException {
//    parent.add(schema.getName(), schema);
//  }

//  @JsonTypeName("proto-group-scan")
//  public static class ProtoGroupScanPop extends AbstractGroupScan {
//
//    private String tableName;
//    private List<String> columns;
//
//    @JsonCreator
//    public ProtoGroupScanPop(@JsonProperty("tableName") String tableName,
//                             @JsonProperty("columns") List<String> columns) {
//      super((String)null);
//      this.tableName = tableName;
//      this.columns = columns;
//    }
//
//    @Override
//    public ScanStats getScanStats() {
//      return ScanStats.TRIVIAL_TABLE;
//    }
//
//    @Override
//    public void applyAssignments(List<DrillbitEndpoint> endpoints)
//        throws PhysicalOperatorSetupException {
//      // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public SubScan getSpecificScan(int minorFragmentId)
//        throws ExecutionSetupException {
//      // TODO Auto-generated method stub
//      return new ProtoSubScanPop(tableName);
//    }
//
//    @Override
//    public int getMaxParallelizationWidth() {
//      return 1;
//    }
//
//    public String getTableName( ) { return tableName; }
//
//    @Override
//    public String getDigest() {
//      return toString();
//    }
//
//    @Override
//    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
//        throws ExecutionSetupException {
//      return new ProtoGroupScanPop(tableName, columns);
//    }
//
//    @Override
//    public GroupScan clone(List<SchemaPath> columns) {
//      List<String> cols = new ArrayList<String>();
//      for (SchemaPath path : columns) {
//        cols.add(path.getAsNamePart().getName());
//      }
//      return new ProtoGroupScanPop(tableName, cols);
//    }
//
//  }

  @JsonTypeName("proto-sub-scan")
  public static class ProtoSubScanPop extends AbstractSubScan {

    private String tableName;

    @JsonCreator
    public ProtoSubScanPop(@JsonProperty("tableName") String tableName) {
      super((String)null);
      this.tableName = tableName;
    }

    public String getTableName( ) { return tableName; }

    @Override
    public int getOperatorType() {
      return 0;
    }
  }

//  @Override
//  protected ProtoExtension createSystem(String schemaName,
//      ProtoPluginConfig config, DrillbitContext context) {
//    // TODO Auto-generated method stub
//    return null;
//  }

}
