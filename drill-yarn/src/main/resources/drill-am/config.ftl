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
  <h4>Fully Resolved Configuration Settings</h4>
  <p>&nbsp;

  <table class="table table-hover" style="width: auto;">
    <tr>
      <th>Configuration Key</td>
      <th>Value</td>
    </tr>
    <#list model as pair>
      <tr>
        <td>${pair.getName()}</td>
        <td>${pair.getQuotedValue()}</td>
      </tr>
    </#list>
  </table>
  <p>
  To modify these values, edit <code>$DRILL_HOME/conf/drill-on-yarn.conf</code>, then rebuild your archive
  and restart your Drill cluster using the Drill-on-YARN client.
</#macro>

<@page_html/>
