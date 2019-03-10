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
package org.apache.drill.exec.physical.rowSet.impl;

import java.util.Collection;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.rowSet.ResultVectorCache;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl.ResultSetOptions;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.convert.ColumnConversionFactory;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

/**
 * Builder for the options for the row set loader. Reasonable defaults
 * are provided for all options; use the default options for test code or
 * for clients that don't need special settings.
 */

public class OptionBuilder {
  protected int vectorSizeLimit;
  protected int rowCountLimit;
  protected Collection<SchemaPath> projection;
  protected RequestedTuple projectionSet;
  protected ResultVectorCache vectorCache;
  protected TupleMetadata schema;
  protected long maxBatchSize;
  protected ColumnConversionFactory conversionFactory;
  protected SchemaTransformer schemaTransformer;

  public OptionBuilder() {
    // Start with the default option values.
    ResultSetOptions options = new ResultSetOptions();
    vectorSizeLimit = options.vectorSizeLimit;
    rowCountLimit = options.rowCountLimit;
    maxBatchSize = options.maxBatchSize;
  }

  /**
   * Specify the maximum number of rows per batch. Defaults to
   * {@link BaseValueVector#INITIAL_VALUE_ALLOCATION}. Batches end either
   * when this limit is reached, or when a vector overflows, whichever
   * occurs first. The limit is capped at {@link ValueVector#MAX_ROW_COUNT}.
   *
   * @param limit the row count limit
   * @return this builder
   */

  public OptionBuilder setRowCountLimit(int limit) {
    rowCountLimit = Math.max(1,
        Math.min(limit, ValueVector.MAX_ROW_COUNT));
    return this;
  }

  public OptionBuilder setBatchSizeLimit(int bytes) {
    maxBatchSize = bytes;
    return this;
  }

  /**
   * Record (batch) readers often read a subset of available table columns,
   * but want to use a writer schema that includes all columns for ease of
   * writing. (For example, a CSV reader must read all columns, even if the user
   * wants a subset. The unwanted columns are simply discarded.)
   * <p>
   * This option provides a projection list, in the form of column names, for
   * those columns which are to be projected. Only those columns will be
   * backed by value vectors; non-projected columns will be backed by "null"
   * writers that discard all values.
   *
   * @param projection the list of projected columns
   * @return this builder
   */

  public OptionBuilder setProjection(Collection<SchemaPath> projection) {
    this.projection = projection;
    return this;
  }

  public OptionBuilder setProjectionSet(RequestedTuple projectionSet) {
    this.projectionSet = projectionSet;
    return this;
  }

  /**
   * Downstream operators require "vector persistence": the same vector
   * must represent the same column in every batch. For the scan operator,
   * which creates multiple readers, this can be a challenge. The vector
   * cache provides a transparent mechanism to enable vector persistence
   * by returning the same vector for a set of independent readers. By
   * default, the code uses a "null" cache which creates a new vector on
   * each request. If a true cache is needed, the caller must provide one
   * here.
   */

  public OptionBuilder setVectorCache(ResultVectorCache vectorCache) {
    this.vectorCache = vectorCache;
    return this;
  }

  /**
   * Clients can use the row set builder in several ways:
   * <ul>
   * <li>Provide the schema up front, when known, by using this method to
   * provide the schema.</li>
   * <li>Discover the schema on the fly, adding columns during the write
   * operation. Leave this method unset to start with an empty schema.</li>
   * <li>A combination of the above.</li>
   * </ul>
   * @param schema the initial schema for the loader
   * @return this builder
   */

  public OptionBuilder setSchema(TupleMetadata schema) {
    this.schema = schema;
    return this;
  }

  /**
   * Provide an optional column conversion factory. Can create a "shim"
   * writer to convert from a type convenient for the client to the type
   * required by the vector. Example: string-to-int conversion. The factory
   * can inspect column metadata properties to decide what conversion, if
   * any, to create.
   * <p>
   * A column conversion factory assumes that the data type of the output
   * vector is the same as the data type of the column schema passed to
   * the result set loader. It does allow inserting a type conversion or
   * validation shim above the column writer specified by the column
   * type.
   * <p>
   * The result set loader makes no assumptions about how the conversion
   * factory works. It can follow the Drill schema definition patterns, or
   * it could do something ad-hoc: such details are hidden inside the
   * implementation.
   * <p>
   * Provide either a (lower-level) column conversion factory or a
   * (higher level) column transformer, but not both. This form is used
   * mostly for testing, but could also be used by operators other than
   * scan,.
   *
   * @param factory a class which can inspect each writer as it is created
   * and insert a new "shim" writer as desired
   * @return this builder
   */
  public OptionBuilder setConversionFactory(ColumnConversionFactory factory) {
    conversionFactory = factory;
    return this;
  }

  /**
   * Provide an optional higher-level schema transformer which can convert
   * columns from one type to another. It is a more powerful version of the
   * column conversion factory.
   * <p>
   * Provide either a (lower-level) column conversion factory or a
   * (higher level) column transformer, but not both.
   *
   * @param transform the column conversion factory
   * @return this builder
   */
  public OptionBuilder setSchemaTransform(SchemaTransformer transform) {
    schemaTransformer = transform;
    return this;
  }

  // TODO: No setter for vector length yet: is hard-coded
  // at present in the value vector.

  public ResultSetOptions build() {
    Preconditions.checkArgument(conversionFactory == null || schemaTransformer == null);
    Preconditions.checkArgument(projection == null || projectionSet == null);
    return new ResultSetOptions(this);
  }
}
