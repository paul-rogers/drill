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
 * Operators require a large variety of services. This package offers
 * the services as a set of interfaces, then provides a wrapper interface
 * that provides access to the collection of services.
 * <p>
 * The design here supports both "production" code (in which all services
 * are available, and are tied to the server as a whole) and test
 * environments in which services are mocked, or are implemented in a
 * way that provides control over operator internals.
 * <p>
 * The implementation here replaces the previous design which evolved
 * to offer a large number of functions in a single concrete class --
 * a solution that works fine in production, but the resulting tight
 * coupling made it very hard to unit test individual components.
 */
package org.apache.drill.exec.ops.services;