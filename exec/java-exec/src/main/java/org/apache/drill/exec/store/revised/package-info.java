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
 * Enhanced, simplified storage interface.
 * <p>
 * Provides a framework for defining three kinds of plugins:
 * <ul>
 * <li>Table space: a name space for tables in either an external system, or it an
 * underlying storage system.</li>
 * <li>Format: a simplified format plugin for any existing storage plugin that works
 * with files.</li>
 * </ul>
 */
package org.apache.drill.exec.store.revised;