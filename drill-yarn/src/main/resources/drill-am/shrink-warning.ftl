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
  <h3>YARN Application Master &ndash; ${clusterName}</h3>
  <h4><#if model.isStop( )>
  Confirm Cluster Shutdown
  <#else>
  Confirm Stopping of Drillbits
  </#if></h4>

  <div class="alert alert-danger">
    <strong>Warning!</strong> You have requested to
    <#if model.isStop()>
    stop the Drill cluster.
    <#else>
    remove ${model.getCount( )}
    <#if model.getCount() == 1>Drillbit<#else>Drillbits</#if>.
    </#if>
    In this version of Drill, stopping Drillbits will
    cause in-flight queries to fail.
  </div>
  <#if model.isStop( )>
  <form method="POST" action="/stop">
  <#else>
  <form method="POST" action="/resize">
  </#if>
  <#if ! model.isStop( )>
    <input type="hidden" name="n" value="${model.getCount( )}">
    <input type="hidden" name="type" value="force-shrink">
  </#if>
  <input type="submit" value="Confirm"> or
  <a href="/">Cancel</a>.
  </form>
</#macro>

<@page_html/>
