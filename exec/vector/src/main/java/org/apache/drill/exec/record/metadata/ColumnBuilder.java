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
package org.apache.drill.exec.record.metadata;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;

public class ColumnBuilder {

  private final String name;
  private final MinorType type;
  private final DataMode mode;
  private int precision = PrimitiveColumnMetadata.UNDEFINED;
  private int scale = PrimitiveColumnMetadata.UNDEFINED;

  public ColumnBuilder(String name, MinorType type, DataMode mode) {
    this.name = name;
    this.type = type;
    this.mode = mode;
    if (type == MinorType.VARDECIMAL) {
      precision = ColumnMetadata.MAX_DECIMAL_PRECISION;
      scale = 0;
    }
  }

  public ColumnBuilder precision(int precision) {
    this.precision = precision;
    return this;
  }

  public ColumnBuilder scale(int scale) {
    this.scale = scale;
    return this;
  }

  public ColumnMetadata build() {
    return new PrimitiveColumnMetadata(name, type, mode, precision, scale);
  }

  public static ColumnMetadata required(String name, MinorType type) {
   return builder(name, type, DataMode.REQUIRED).build();
  }

  public static ColumnMetadata nullable(String name, MinorType type) {
   return builder(name, type, DataMode.OPTIONAL).build();
  }

  public static ColumnBuilder builder(String name, MinorType type, DataMode mode) {
    return new ColumnBuilder(name, type, mode);
  }

  public static ColumnMetadata build(String name, MajorType type) {
    ColumnBuilder builder = builder(name, type.getMinorType(), type.getMode());
    if (type.hasPrecision() && type.getPrecision() > 0) {
      builder.precision(type.getPrecision());
      if (type.hasScale()) {
        builder.scale(type.getScale());
      }
    }
    return builder.build();
  }
}
