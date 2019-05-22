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
package org.apache.drill.exec.physical.impl.scan.project.projSet;

import java.util.Map;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple.RequestedColumn;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.convert.ColumnConversionFactory;
import org.apache.drill.exec.vector.accessor.convert.StandardConversions;
import org.apache.drill.exec.vector.accessor.convert.StandardConversions.ConversionDefn;
import org.apache.drill.exec.vector.accessor.convert.StandardConversions.ConversionType;

/**
 * Base class for projection sets that include an output schema. In this
 * case, the output schema provides additional constraints for a reader
 * column: the type can be converted from reader type to "provided"
 * type.
 */

public abstract class AbstractSchemaProjectionSet extends AbstractProjectionSet {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(AbstractSchemaProjectionSet.class);

  protected final TupleMetadata outputSchema;
  private final CustomTypeTransform customTransform;
  protected final Map<String, String> properties;

  public AbstractSchemaProjectionSet(TupleMetadata outputSchema,
      Map<String, String> properties, CustomTypeTransform customTransform) {
    this.outputSchema = outputSchema;
    this.customTransform = customTransform;
    this.properties = properties;
  }

  protected SchemaReadColProj schemaReadProj(ColumnMetadata inputSchema, RequestedColumn reqCol, ColumnMetadata outputCol) {
    return new SchemaReadColProj(inputSchema, reqCol, outputCol,
        conversionFactory(inputSchema, outputCol));
  }

  private ColumnConversionFactory conversionFactory(ColumnMetadata inputSchema, ColumnMetadata outputCol) {

    ConversionDefn defn = StandardConversions.analyze(inputSchema, outputCol);
    if (defn.type == ConversionType.NONE) {
      return null;
    }
    if (customTransform != null) {
      ColumnConversionFactory factory = customTransform.transform(inputSchema, outputCol, defn);
      if (factory != null) {
        return factory;
      }
    }
    if (defn.type == ConversionType.IMPLICIT) {
      return null;
    }
    if (defn.conversionClass == null) {
      throw UserException.validationError()
        .message("Runtime type conversion not available")
        .addContext("Input type", inputSchema.typeString())
        .addContext("Output type", outputCol.typeString())
        .build(logger);
    }
    return StandardConversions.factory(defn.conversionClass, properties);
  }
}
