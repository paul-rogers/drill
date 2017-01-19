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

import java.util.List;

import org.apache.drill.exec.store.revised.Sketch.FilterExpr;
import org.apache.drill.exec.store.revised.Sketch.LogicalSchema;
import org.apache.drill.exec.store.revised.Sketch.LogicalTable;
import org.apache.drill.exec.store.revised.Sketch.RowSchema;
import org.apache.drill.exec.store.revised.Sketch.StorageSpace;
import org.apache.drill.exec.store.revised.Sketch.TableScan;
import org.apache.drill.exec.store.revised.plan.BaseImpl.*;
import org.apache.drill.exec.store.revised.retired.ExtendableLogicalScanPop;
import org.apache.drill.exec.store.revised.retired.StorageExtension;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

public class ProtoExtension /* extends BaseExtension */ {

//  public ProtoExtension(String schemaName) {
//    super(new AbstractLogicalSchema(schemaName, new ProtoSchemaReader(), null));
//  }

//  public static class ProtoSchema extends AbstractLogicalSchema {
//
//    private ProtoSchemaReader reader = new ProtoSchemaReader();
//
//    public ProtoSchema(String schemaName) {
//      super(schemaName);
//    }
//
//    @Override
//    public SchemaReader reader() {
//       return reader;
//    }
//  }

//  public static class ProtoSchemaReader extends AbstractSchemaReader {
//
//    @Override
//    public TableScan scan(LogicalTable table) {
//      // TODO Auto-generated method stub
//      return null;
//    }
//
//
//  }

//  @JsonTypeName("proto-table")
//  public static class ProtoTableMemento {
//
//    private String name;
//
//    public ProtoTableMemento(@JsonProperty("tableName") String tableName) {
//      name = tableName;
//    }
//
//    public String getTableName( ) { return name; }
//  }

//  public static class ProtoTable extends AbstractLogicalTable {
//
//    public ProtoTable(String name) {
//      super(name);
//    }
//  }
}
