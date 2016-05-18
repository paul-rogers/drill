<#-- Licensed to the Apache Software Foundation (ASF) under one or more contributor
  license agreements. See the NOTICE file distributed with this work for additional
  information regarding copyright ownership. The ASF licenses this file to
  You under the Apache License, Version 2.0 (the "License"); you may not use
  this file except in compliance with the License. You may obtain a copy of
  the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required
  by applicable law or agreed to in writing, software distributed under the
  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
  OF ANY KIND, either express or implied. See the License for the specific
  language governing permissions and limitations under the License. -->

<#include "*/generic.ftl">
<#macro page_head>
</#macro>

<#macro page_body>
  <h4>Drill Cluster Status</h4>
  <p>&nbsp;

  <table class="table table-hover" style="width: auto;">
    <tr>
      <td>YARN Application ID</td>
      <td><a href="${model.getRmAppLink( )}">${model.getAppId( )}</a></td>
    </tr>
    <tr>
      <td>YARN Resource Manager</td>
      <td><a href="${model.getRmLink( )}">${model.getRmHost( )}</a></td>
    </tr>
    <tr>
      <td>YARN Node Manager for AM</td>
      <td><a href="${model.getNmLink( )}">${model.getNmHost( )}</a> |
          <a href="${model.getNmAppLink( )}">App info</a></td>
    </tr>
    <tr>
      <td>State</td>
      <td>${model.getState( )}</td>
    </tr>
    <tr>
      <td>Target Drillbit Count</td>
      <td>${model.getTargetCount( )}</td>
    </tr>
    <tr>
      <td>Live Drillbit Count</td>
      <td>${model.getLiveCount( )}</td>
    </tr>
    <tr>
      <td>Total Drill Virtual Cores</td>
      <td>${model.getDrillTotalVcores( )}</td>
    </tr>
    <tr>
      <td>Total Drill Memory (MB)</td>
      <td>${model.getDrillTotalMemory( )}</td>
    </tr>
    <tr>
      <td>Yarn Node Count</td>
      <td>${model.getYarnNodeCount( )}</td>
    </tr>
    <tr>
      <td>Total Yarn Virtual Cores</td>
      <td>${model.getYarnVcores( )}</td>
    </tr>
    <tr>
      <td>Total Yarn Memory (MB)</td>
      <td>${model.getYarnMemory( )}</td>
    </tr>
  </table>
  <table class="table table-hover" style="width: auto;">
    <tr>
      <th>Pool</th>
      <th>Type</th>
      <th>Target Drillbit Count</th>
      <th>Total Drillbits</th>
      <th>Live Drillbits</th>
      <th>Memory per Drillbit (MB)</th>
      <th>VCores per Drillbit</th>
    </tr>
    <#list model.getPools( ) as pool>
      <tr>
        <td>${pool.getName( )}</td>
        <td>${pool.getType( )}</td>
        <td>${pool.getTargetCount( )}</td>
        <td>${pool.getTaskCount( )}</td>
        <td>${pool.getLiveCount( )}</td>
        <td>${pool.getMemory( )}</td>
        <td>${pool.getVcores( )}</td>
      </tr>
    </#list>
  </table>
</#macro>

<@page_html/>
