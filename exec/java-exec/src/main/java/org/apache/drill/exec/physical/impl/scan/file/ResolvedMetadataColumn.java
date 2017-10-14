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

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.project.ColumnProjection;
import org.apache.drill.exec.physical.impl.scan.project.ResolvedColumn;
import org.apache.drill.exec.physical.impl.scan.project.UnresolvedColumn;

public abstract class ResolvedMetadataColumn extends ResolvedColumn {

  public static class ResolvedFileMetadataColumn extends ResolvedMetadataColumn {

    public static final int ID = 15;

    private final FileMetadataColumnDefn defn;

    public ResolvedFileMetadataColumn(String name, UnresolvedColumn source, FileMetadataColumnDefn defn,
        FileMetadata fileInfo) {
      super(name, defn.dataType(), source,
          defn.defn.getValue(fileInfo.filePath()));
      this.defn = defn;
    }

    @Override
    public int nodeType() { return ID; }

    @Override
    public ColumnProjection unresolve() {
      return new UnresolvedFileMetadataColumn(source().source(), defn);
    }
  }

  public static class ResolvedPartitionColumn extends ResolvedMetadataColumn {

    public static final int ID = 16;

    protected final int partition;

    public ResolvedPartitionColumn(String name, int partition, FileMetadata fileInfo) {
      super(name, dataType(), fileInfo.partition(partition));
      this.partition = partition;
    }

    @Override
    public int nodeType() { return ID; }
    public int partition() { return partition; }

    public static MajorType dataType() {
      return MajorType.newBuilder()
            .setMinorType(MinorType.VARCHAR)
            .setMode(DataMode.OPTIONAL)
            .build();
    }

    @Override
    public ColumnProjection unresolve() {
      return new UnresolvedPartitionColumn(source().source(), partition, name());
    }
  }

  private final String value;

  public ResolvedMetadataColumn(String name, MajorType type, String value) {
    super(name, type);
    this.value = value;
  }

  public String value() { return value; }
}
