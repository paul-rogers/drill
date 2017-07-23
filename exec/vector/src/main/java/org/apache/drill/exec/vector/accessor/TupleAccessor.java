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
package org.apache.drill.exec.vector.accessor;

import org.apache.drill.exec.record.TupleMetadata;

/**
 * Provides access to a "tuple". In Drill, both rows and maps are
 * tuples: both are an ordered collection of values, defined by a
 * schema. Each tuple has a schema that defines the column ordering
 * for indexed access. Each tuple also provides methods to get column
 * accessors by name or index.
 */

public interface TupleAccessor {
  TupleMetadata schema();
}
