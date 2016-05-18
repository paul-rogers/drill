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
  <h4><#if model.getType( ) == "stopped">
  Stop Drill Cluster
  <#else>
  Resize Drill Cluster
  </#if></h4>
  <p>&nbsp;

  <#if model.getType( ) == "resized">
    <div class="alert alert-success">
      <strong>Success!</strong> Cluster resizing to ${model.getValue( )} nodes.
    </div>
  <#elseif model.getType( ) == "null-resize">
    <div class="alert alert-info">
      <strong>Note:</strong> The new size of ${model.getValue( )} is the
      same as the current cluster size.
    </div>
  <#elseif model.getType( ) == "invalid-resize">
    <div class="alert alert-danger">
      <strong>Error!</strong> Invalid cluster resize level: ${model.getValue( )}.
      Please <a href="/manage">try again</a>.
    </div>
  <#elseif model.getType( ) == "invalid-grow">
    <div class="alert alert-danger">
      <strong>Error!</strong> Invalid cluster grow amount: ${model.getValue( )}.
      Please <a href="/manage">try again</a>.
    </div>
  <#elseif model.getType( ) == "invalid-shrink">
    <div class="alert alert-danger">
      <strong>Error!</strong> Invalid cluster shrink amount: ${model.getValue( )}.
      Please <a href="/manage">try again</a>.
    </div>
  <#elseif model.getType( ) == "stopped">
    <div class="alert alert alert-success">
      <strong>Success!</strong> Cluster is shutting down.
    </div>
    Pages on this site will be unavailable until the cluster restarts.
  </#if>
  <#if model.getType( ) != "stopped">
    Return to the <a href="/manage">Management page</a>.
  </#if>
</#macro>

<@page_html/>
