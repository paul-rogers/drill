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

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.revised.Sketch;
import org.apache.drill.exec.store.revised.Sketch.SchemaId;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ExtendableLogicalScanPop<P> extends AbstractGroupScan {

  private P properties;
  private String tableId;
  private List<String> columns;
  private SchemaId schemaId;

  public ExtendableLogicalScanPop(
      @JsonProperty("userName") String userName,
      @JsonProperty("schemaId") SchemaId schemaId,
      @JsonProperty("tableId" ) String tableId,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("properties") P properties ) {
    super( userName );
    this.schemaId = schemaId;
    this.tableId = tableId;
    this.columns = columns;
    this.properties = properties;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints)
      throws PhysicalOperatorSetupException {
    // TODO Auto-generated method stub

  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId)
      throws ExecutionSetupException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getMaxParallelizationWidth() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String getDigest() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
      throws ExecutionSetupException {
    // TODO Auto-generated method stub
    return null;
  }

}
