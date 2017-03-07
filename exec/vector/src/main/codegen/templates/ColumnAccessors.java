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

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/drill/exec/vector/accessor/ColumnAccessors.java" />


<#include "/@includes/license.ftl" />

package org.apache.drill.exec.vector.accessor;

import org.apache.drill.exec.vector.accessor.ColumnAccessor.RowIndex;
import org.apache.drill.exec.vector.*;

import com.google.common.base.Charsets;

/**
 * Basic accessors for most Drill vector types and modes. These are bare-bones
 * accessors: they do only the most rudimentary type conversions. For all,
 * there is only one way to get/set values; they don't convert from, say,
 * a double to an int or visa-versa.
 */

// This class is generated using freemarker and the ${.template_name} template.

public class ColumnAccessors {

  <#list vv.types as type>
  <#list type.minor as minor>
    <#assign drillType=minor.class>
    <#assign javaType=minor.javaType!type.javaType>
    <#assign accessorType=minor.accessorType!type.accessorType!javaType>
    <#assign label=minor.accessorLabel!type.accessorLabel!accessorType?capitalize>
    <#assign notyet=minor.accessorDisabled!type.accessorDisabled!false>
    <#assign cast=minor.accessorCast!minor.accessorCast!type.accessorCast!"none">
    <#if drillType == "VarChar" || drillType == "Var16Char">
      <#assign accessorType="String">
      <#assign label=accessorType>
    </#if>
    <#if ! notyet>

  //------------------------------------------------------------------------
  // ${drillType} readers and writers

  public static class ${drillType}ColumnReader extends AbstractColumnReader {

    private ${drillType}Vector.Accessor accessor;

    @Override
    public void bind(RowIndex rowIndex, ValueVector vector) {
      bind(rowIndex);
      this.accessor = ((${drillType}Vector) vector).getAccessor();
    }

    @Override
    public ${accessorType} get${label}() {
      <#if drillType == "VarChar">
      return new String(accessor.get(rowIndex()), Charsets.UTF_8);
      <#elseif drillType == "Var16Char">
      return new String(accessor.get(rowIndex()), Charsets.UTF_16);
      <#elseif drillType == "VarBinary">
      return accessor.get(rowIndex());
      <#else>
      return accessor.get(rowIndex());
      </#if>
    }
  }

  public static class Nullable${drillType}ColumnReader extends AbstractColumnReader {

    private Nullable${drillType}Vector.Accessor accessor;

    @Override
    public void bind(RowIndex rowIndex, ValueVector vector) {
      bind(rowIndex);
      this.accessor = ((Nullable${drillType}Vector) vector).getAccessor();
    }

    @Override
    public boolean isNull() {
      return accessor.isNull(rowIndex());
    }

    @Override
    public ${accessorType} get${label}() {
      <#if drillType == "VarChar">
      return new String(accessor.get(rowIndex()), Charsets.UTF_8);
      <#elseif drillType == "Var16Char">
      return new String(accessor.get(rowIndex()), Charsets.UTF_16);
      <#elseif drillType == "VarBinary">
      return accessor.get(rowIndex());
      <#else>
      return accessor.get(rowIndex());
      </#if>
    }
  }

  public static class ${drillType}ColumnWriter extends AbstractColumnWriter {

    private ${drillType}Vector.Mutator mutator;

    @Override
    public void bind(RowIndex rowIndex, ValueVector vector) {
      bind(rowIndex);
      this.mutator = ((${drillType}Vector) vector).getMutator();
    }

    @Override
    public void set${label}(${accessorType} value) {
      <#if drillType == "VarChar">
      byte bytes[] = value.getBytes(Charsets.UTF_8);
      mutator.setSafe(rowIndex(), bytes, 0, bytes.length);
      <#elseif drillType == "Var16Char">
      byte bytes[] = value.getBytes(Charsets.UTF_16);
      mutator.setSafe(rowIndex(), bytes, 0, bytes.length);
      <#else>
      mutator.setSafe(rowIndex(), <#if cast=="set">(${javaType}) </#if>value);
      </#if>
    }
  }

  public static class Nullable${drillType}ColumnWriter extends AbstractColumnWriter {

    private Nullable${drillType}Vector.Mutator mutator;

    @Override
    public void bind(RowIndex rowIndex, ValueVector vector) {
      bind(rowIndex);
      this.mutator = ((Nullable${drillType}Vector) vector).getMutator();
    }

    @Override
    public void setNull() {
      mutator.setNull(rowIndex());
    }

    @Override
    public void set${label}(${accessorType} value) {
      <#if drillType == "VarChar">
      byte bytes[] = value.getBytes(Charsets.UTF_8);
      mutator.setSafe(rowIndex(), bytes, 0, bytes.length);
      <#elseif drillType == "Var16Char">
      byte bytes[] = value.getBytes(Charsets.UTF_16);
      mutator.setSafe(rowIndex(), bytes, 0, bytes.length);
      <#elseif drillType == "VarBinary">
      mutator.setSafe(rowIndex(), value, 0, value.length);
      <#else>
      mutator.setSafe(rowIndex(), <#if cast=="set">(${javaType}) </#if>value);
      </#if>
    }
  }

    </#if>
  </#list>
</#list>

}
