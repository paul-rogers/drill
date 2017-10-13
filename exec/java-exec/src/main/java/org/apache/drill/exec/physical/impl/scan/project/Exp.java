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
package org.apache.drill.exec.physical.impl.scan.project;

import java.util.List;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.physical.impl.scan.managed.SchemaNegotiator;

public class Exp {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Exp.class);

  public interface ReaderLifecycle {

    SchemaNegotiator start();
    void startBatch();
    void endBatch();
    void endReader();
  }

  public interface UnresolvedProjection {
    boolean isProjectAll();
    List<ColumnProjection> outputCols();
  }

  public interface ResolvedProjection {
    List<ResolvedColumn> outputCols();
  }

  public static class ResolvedColumn {

    /**
     * Column name. Output columns describe top-level columns in
     * the project list; so the name here is the root name. If the
     * column represents a map, then the name is the name of the map
     * itself.
     */

    protected final String name;

    /**
     * Column data type.
     */

    private final MajorType type;

//    protected Object extension;

     public ResolvedColumn(String name, MajorType type) {
      this.name = name;
      this.type = type;
    }

     public String name() { return name; }
     public MajorType type() { return type; }
  }

//  public interface SchemaManager extends ScanSchema {
//    void bind(VectorContainer container);
//    @Override
//    void publish();
//  }


//  public static class BaseSchemaManager implements SchemaManager {
//
//    @Override
//    public void bind(VectorContainer container) {
//      // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public void publish() {
//      // TODO Auto-generated method stub
//
//    }
//
//  }
//
//  public static class FileSchemaManager extends BaseSchemaManager {
//
//  }
//
//  public static class TextFileSchemaManager extends FileSchemaManager {
//
//  }
}
