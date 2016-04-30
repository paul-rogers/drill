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
  <h3>&nbsp;</h3>
  <h3>YARN Application Master &ndash; ${clusterName}</h3>
  <h4>Drillbit Status</h4>
  <p>&nbsp;

  <div class="table-responsive">
    <table class="table table-hover">
      <tr>
        <th>ID</th>
        <th>Pool</th>
        <th>Host</th>
        <th>State</th>
        <th>ZK State</th>
        <th>Memory (MB)</th>
        <th>Virtual Cores</th>
        <th>Start Time</th>
      </th>
       <#assign count=0>
       <#list model as task>
        <#assign count=count+1>
        <tr>
          <td><b>${task.getTaskId( )}</b></td>
          <td>${task.getPoolName( )}</td>
          <td>
          <#if task.isLive( )><a href="${task.getLink( )}"></#if>
          ${task.getHost( )}
          <#if task.isLive( )></a></#if>
          </td>
          <td>${task.getState( )}</td>
          <td>${task.getTrackingState( )}</td>
          <td>${task.getMemory( )}</td>
          <td>${task.getVcores( )}</td>
          <td>${task.getStartTime( )}</td>
        </tr>
      </#list>
    </table>
    <#if count == 0>
    No drillbits are running.
    </#if>
  </div>
</#macro>

<@page_html/>
