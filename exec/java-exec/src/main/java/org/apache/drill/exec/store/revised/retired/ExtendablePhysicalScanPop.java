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
package org.apache.drill.exec.store.revised.retired;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.AbstractSubScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("extendable-sub-scan")
public class ExtendablePhysicalScanPop<P> extends AbstractSubScan {

  private String extensionId;
  private P properties;
  private String tableId;
  private List<String> columns;

  public ExtendablePhysicalScanPop(
      @JsonProperty("userName") String userName,
      @JsonProperty("extensionId") String extensionId,
      @JsonProperty("tableId" ) String tableId,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("properties") P properties ) {
    super( userName );
    this.extensionId = extensionId;
    this.tableId = tableId;
    this.columns = columns;
    this.properties = properties;
  }

  @Override
  public <T, X, E extends Throwable> T accept(
      PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
      throws ExecutionSetupException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getOperatorType() {
    return 0; //CoreOperatorType.EXTENDABLE_SUB_SCAN;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return new ArrayList<PhysicalOperator>().iterator();
  }

  public String getExtensionId() {
    return extensionId;
  }

  public String getTableId() {
    return tableId;
  }

  public List<String> getColumns() {
    return columns;
  }

  public P getProperties() {
    return properties;
  }

}