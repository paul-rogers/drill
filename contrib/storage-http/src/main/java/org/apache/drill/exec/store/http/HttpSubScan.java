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
package org.apache.drill.exec.store.http;

import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;
import org.apache.drill.exec.proto.UserBitShared;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpSubScan extends AbstractBase implements SubScan {
  static final Logger logger = LoggerFactory.getLogger(HttpSubScan.class);

  private HttpScanSpec scanSpec;
  private HttpStoragePluginConfig config;
  private final List<SchemaPath> columns;

  public HttpSubScan(@JsonProperty("HttpPluginConfig") HttpStoragePluginConfig config,
                     @JsonProperty("tabletScanSpecList") HttpScanSpec spec,
                     @JsonProperty("columns") List<SchemaPath> columns) {
    scanSpec = spec;
    this.config = config;
    this.columns = columns;
  }

  HttpScanSpec getScanSpec() {
    return scanSpec;
  }

  String getURL() {
    return scanSpec.getURL();
  }

  String getFullURL() {
    return config.getConnection() + getURL();
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }


  HttpStoragePluginConfig getStorageConfig() {
    return config;
  }

  @Override
  public <T, X, E extends Throwable> T accept(
    PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
    throws ExecutionSetupException {
    return new HttpSubScan(config, scanSpec, columns);
  }

  @Override
  public int getOperatorType() {
    return UserBitShared.CoreOperatorType.HTTP_SUB_SCAN_VALUE;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return ImmutableSet.<PhysicalOperator>of().iterator();
  }
}
