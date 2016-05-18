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
  <h4>Manage Drill Cluster</h4>

  Current Status: ${model.getLiveCount( )} Drillbits running
  <p><p>
  Management Actions:
  <form action="/resize" method="POST">
  <input hidden name="type" value="grow">
  Add &nbsp;&nbsp;
  <input type="text" name="n" size="6"> &nbsp;&nbsp; drillbits.
  &nbsp;&nbsp; <input type="submit" value="Go">
  </form>
  <p>
  <form action="/resize" method="POST">
  <input hidden name="type" value="shrink">
  Remove &nbsp;&nbsp;
  <input type="text" name="n" size="6">  &nbsp;&nbsp; drillbits.
  &nbsp;&nbsp; <input type="submit" value="Go">
  </form>
  <p>
  <form action="/resize" method="POST">
  <input hidden name="type" value="resize">
  Resize to &nbsp;&nbsp;
  <input type="text" name="n" size="6"> &nbsp;&nbsp; drillbits.
  &nbsp;&nbsp; <input type="submit" value="Go">
  </form>
  <p>
  <form action="/stop" method="GET">Stop the Cluster.
  &nbsp;&nbsp; <input type="submit" value="Go">
  </form>

</#macro>

<@page_html/>
