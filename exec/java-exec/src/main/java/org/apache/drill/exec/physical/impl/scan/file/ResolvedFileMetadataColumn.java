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
package org.apache.drill.exec.physical.impl.scan.file;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.project.ColumnProjection;
import org.apache.drill.exec.physical.impl.scan.project.ConstantColumn;

public class ResolvedFileMetadataColumn extends ConstantColumn {

  public static final int ID = 15;

  private final FileMetadataColumnDefn defn;

  public ResolvedFileMetadataColumn(String name, SchemaPath source, FileMetadataColumnDefn defn,
      FileMetadata fileInfo) {
    super(name, defn.dataType(), source, defn.defn.getValue(fileInfo.filePath()));
    this.defn = defn;
  }

  @Override
  public int nodeType() { return ID; }

  @Override
  public ColumnProjection unresolve() {
    return new UnresolvedFileMetadataColumn(source(), defn);
  }
}