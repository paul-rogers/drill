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
import org.apache.drill.exec.physical.impl.scan.file.ResolvedMetadataColumn.ResolvedPartitionColumn;
import org.apache.drill.exec.physical.impl.scan.project.UnresolvedColumn;

public class UnresolvedPartitionColumn extends UnresolvedColumn {

  public static final int ID = 10;

  private final int partition;
  private final String generatedName;

  public UnresolvedPartitionColumn(SchemaPath inCol, int partition, String generatedName) {
    super(inCol, ID);
    this.partition = partition;
    this.generatedName = generatedName;
  }

  public int partition() { return partition; }

  public ResolvedPartitionColumn resolve(FileMetadata fileInfo) {
    String resolvedName = inCol.isWildcard() ? generatedName : inCol.rootName();
    return new ResolvedPartitionColumn(resolvedName, this, partition, fileInfo);
  }

  @Override
  protected void buildString(StringBuilder buf) {
    buf.append(", partition=")
       .append(partition);
  }
}