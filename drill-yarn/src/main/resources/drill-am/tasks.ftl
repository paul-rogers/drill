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
        <th>Container</th>
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
          <#if task.isLive( )>
            <a href="${task.getLink( )}" data-toggle="tooltip" title="Drillbit Web UI"></#if>
          ${task.getHost( )}
          <#if task.isLive( )></a></#if>
          </td>
          <td> ${task.getState( )}
          <#if task.isCancelled( )><br/>(Cancelled)</#if>
          <#if task.isCancellable( )>
            <a href="/cancel?id=${task.getTaskId( )}" data-toggle="tooltip" title="Kill this Drillbit">[x]</a>
          </#if>
          </td>
          <td>${task.getTrackingState( )}</td>
          <td><#if task.hasContainer( )>
            <a href="${task.getNmLink( )}" data-toggle="tooltip" title="Node Manager UI for Drillbit container">${task.getContainerId()}</a>
          <#else>&nbsp;</#if></td>
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
