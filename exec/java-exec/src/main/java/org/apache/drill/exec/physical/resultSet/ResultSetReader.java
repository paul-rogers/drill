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
package org.apache.drill.exec.physical.resultSet;

import org.apache.drill.exec.physical.rowSet.RowSetReader;
import org.apache.drill.exec.record.metadata.TupleMetadata;

/**
 * Iterates over a query result set as a set of batches,
 * each of which is iterated over via a row set reader.
 * Protocol:
 * <ul>
 * <li>Create the result set reader via a specific subclass.
 * If a query has a null result (no rows,
 * no schema), the code which creates this class should instead
 * indicate that no results are available. This class is only for
 * the cases </li>
 * <li>Call {@link #schema()}, if desired, to obtain the schema
 * for this result set.</li>
 * <li>Call {@link #next()} to advance to the first batch.</li>
 * <li>If {@code next()} returns {@code true}, then call
 * {@link #reader()} to obtain a reader over rows. This reader also
 * provides the batch schema.</li>
 * <li>Use the reader to iterate over rows in the batch.</li>
 * <li>Call {@code next()} to advance to the next batch and
 * repeat.</li>
 * </ul>
 * <p>
 * The implementation may perform complex tasks behind the scenes:
 * coordinate with the query runner (if remote), drive an operator
 * (if within a DAG), etc.
 * <p>
 * This version <i>does not</i> handle schema changes: it assumes
 * that either the query returns a uniform result set or that schema
 * changes can occur "silently". If a query can return multiple
 * schemas, then another iterator should iterate over the disjoint
 * sub-sets, and use this iterator for spans of batches with the
 * same schema.
 * <p>
 * Depending on context, the underlying mechanism may hold resources
 * which must be released. That is the responsibility of a
 * context-specific mechanism as it differs between a client consuming
 * query output and an operator consuming batches from its upstream
 * child.
 */
public interface ResultSetReader {

  /**
   * Return the schema for this result set.
   */
  TupleMetadata schema();

  /**
   * Advance to the next batch of data. The iterator starts
   * positioned before the first batch (but after obtaining
   * a schema.)
   * @return {@code true} if another batch is available,
   * {@code false} if EOF
   */
  boolean next();

  /**
   * Obtain a reader to iterate over the rows of the batch. The return
   * value will likely be the same reader each time, so that this call
   * is optional after the first batch.
   */
  RowSetReader reader();
}
