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

/**
 * The dynamic projection in Drill is complex. With the advent of
 * provided schema, we now have many ways to manage projection. The
 * classes here implement these many policies. They are implemnted
 * as distinct classes (rather than chains of if-statements) to
 * make the classes easier to test and reason about.
 * <p>
 * Projection is a combination of three distinct policies:
 * <ul>
 * <li>Projection policy (all, none, explicit, etc.)</li>
 * <li>Column policy (unprojected, explicit projection,
 * projection with schema, etc.)</li>
 * <li>Type conversion: none, based on a provided schema,
 * custom.</li>
 * </ul>
 * Experience has shown that these must be separated: each is designed
 * and tested separately to keep the problem tractable.
 *
 * <h4>Projection Set Cases</h4>
 *
 * The project cases and their classes:
 * <p>
 * <dl>
 * <dt>{@link EmptyProjectionSet}</dt>
 * <dd><tt>SELECT COUNT(*)</tt>: Project nothing. Only count records.</dd>
 * <dl>
 * <dt>{@link WildcardProjectionSet}</dt>
 * <dd><tt>SELECT *</tt>: Project everything, when no schema is provided.
 * However, don't project columns which have been marked as
 * special: {@link ColumnMetadata#EXCLUDE_FROM_WILDCARD}.</dd>
 * <dt>{@link ExplicitProjectionSet}</dt>
 * <dd><tt>SELECT a, b[10], c.d</tt>: Explicit projection without
 * a schema. Project only the selected columns. Verify that the reader
 * provides column types/modes consistent with the implied form in the
 * projection list. That is, in this example, `b` must be an array.</dd>
 * <dt>{@link WildcardAndSchemaProjectionSet}</dt>
 * <dd><tt>SELECT *</tt> with a schema. Columns are projected if not
 * special, and (if strict schema) only in the output schema.</dd>
 * <dt>{@link WildcardAndTransformProjectionSet}<dt>
 * <dd><tt>SELECT *</tt> without a schema, but with custom rules for
 * type conversion. Used primarily for testing.</dd>
 * </dl>
 *
 * <h4>Column Projection Cases</h4>
 *
 * Each projection set answers a query: "the reader wants to add such-and-so
 * column: what should I do?" Since the reader is free to add any column,
 * we don't cache the list of columns as is done with the parsed project
 * list, or the output schema. Instead, we handle each column on a
 * case-by-case basis; we create a {@link ColumnReadProjection} instance
 * to answer the query. Instances of this class are meant to be transient:
 * use them and discard them. We answer the query differently depending on
 * many factors, including:
 * <p>
 * <dl>
 * <dt>{@link UnprojectedReadColProj}</dt>
 * <dd>Column is not projected. Nothing to convert, no type checks
 * needed. The result set loader should create a dummy writer for this
 * case.</dd>
 * <dt>{@link ExplicitReadColProj}</dt>
 * <dd>The column is projected based on an item in an explicit projection
 * list. Verify that the type matches the implied type from the projection
 * list. No schema is available, so no type conversion.</dd>
 * <dt>{@link SchemaReadColProj}</dt>
 * <dd>Explicit projection of a column backed by a provided schema. May
 * be backed by an explicit projection item. Or, this column may be the
 * result of a wildcard expansion for a strict schema. The schema provides
 * type conversion. The explicit projection item provides additional
 * constraints.</dd>
 * <dt>{@link TransformReadColProj}</dt>
 * <dd>Explicit or wildcard column for the case of a custom transform without
 * a schema.</dd>
 * <dt>{@link WildcardReadColProj}</dt>
 * <dd>Wildcard column: no type or transform, so no checks available.</dd>
 * </dl>
 *
 * <h4>Construction</h4>
 *
 * Two classes build the above complex cases:
 * <p>
 * <dl>
 * <dt>{@link ProjectionSetFactory}<dt>
 * <dd>Builds simple projection sets that take few parameters.</dd>
 * <dt>{@link ProjectionSetBuilder}</dt>
 * <dd>Handles the complex cases.</dd>
 */

package org.apache.drill.exec.physical.impl.scan.project.projSet;
