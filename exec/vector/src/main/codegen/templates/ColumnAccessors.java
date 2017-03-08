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
<#macro getType label>
    @Override
    public ValueType getType() {
  <#if label == "Int">
      return ValueType.INTEGER;
  <#else>
      return ValueType.${label?upper_case};
  </#if>
    }
</#macro>
<#macro bindReader prefix drillType>
  <#if drillType = "Decimal9" || drillType == "Decimal18">
    private MaterializedField field;
  </#if>
    private ${prefix}${drillType}Vector.Accessor accessor;

    @Override
    public void bind(RowIndex rowIndex, ValueVector vector) {
      bind(rowIndex);
  <#if drillType = "Decimal9" || drillType == "Decimal18">
      field = vector.getField();
  </#if>
      this.accessor = ((${prefix}${drillType}Vector) vector).getAccessor();
    }
</#macro>
<#macro get drillType accessorType label>
    @Override
    public ${accessorType} get${label}() {
  <#if drillType == "VarChar">
      return new String(accessor.get(rowIndex()), Charsets.UTF_8);
  <#elseif drillType == "Var16Char">
      return new String(accessor.get(rowIndex()), Charsets.UTF_16);
  <#elseif drillType == "VarBinary">
      return accessor.get(rowIndex());
  <#elseif drillType == "Decimal9" || drillType == "Decimal18">
      return DecimalUtility.getBigDecimalFromPrimitiveTypes(
                accessor.get(rowIndex()),
                field.getScale(),
                field.getPrecision());
  <#elseif accessorType == "Decimal18">
      return DecimalUtilities.getBigDecimalFromPrimitiveTypes(accessor.getObject(rowIndex());
  <#elseif accessorType == "BigDecimal">
      return accessor.getObject(rowIndex());
  <#else>
      return accessor.get(rowIndex());
  </#if>
    }
</#macro>
<#macro bindWriter prefix drillType>
  <#if drillType = "Decimal9" || drillType == "Decimal18">
    private MaterializedField field;
  </#if>
    private ${prefix}${drillType}Vector.Mutator mutator;

    @Override
    public void bind(RowIndex rowIndex, ValueVector vector) {
      bind(rowIndex);
  <#if drillType = "Decimal9" || drillType == "Decimal18">
      field = vector.getField();
  </#if>
      this.mutator = ((${prefix}${drillType}Vector) vector).getMutator();
    }
</#macro>
<#macro set drillType accessorType label nullable>
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
  <#elseif drillType == "Decimal9">
      mutator.setSafe(rowIndex(),
          DecimalUtility.getDecimal9FromBigDecimal(value,
              field.getScale(), field.getPrecision()));
  <#elseif drillType == "Decimal18">
      mutator.setSafe(rowIndex(),
          DecimalUtility.getDecimal18FromBigDecimal(value,
              field.getScale(), field.getPrecision()));
  <#else>
      mutator.setSafe(rowIndex(), <#if cast=="set">(${javaType}) </#if>value);
  </#if>
    }
</#macro>

package org.apache.drill.exec.vector.accessor;

import java.math.BigDecimal;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.vector.*;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.util.DecimalUtility;

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
    <#assign accessorType=minor.accessorType!type.accessorType!minor.friendlyType!javaType>
    <#assign label=minor.accessorLabel!type.accessorLabel!accessorType?capitalize>
    <#assign notyet=minor.accessorDisabled!type.accessorDisabled!false>
    <#assign cast=minor.accessorCast!minor.accessorCast!type.accessorCast!"none">
    <#assign friendlyType=minor.friendlyType!"">
    <#if accessorType=="BigDecimal">
      <#assign label="Decimal">
    </#if>
    <#if ! notyet>

  //------------------------------------------------------------------------
  // ${drillType} readers and writers

  public static class ${drillType}ColumnReader extends AbstractColumnReader {

    <@bindReader "" drillType />

    <@getType label />

    <@get drillType accessorType label />
  }

  public static class Nullable${drillType}ColumnReader extends AbstractColumnReader {

    <@bindReader "Nullable" drillType />

    <@getType label />

    @Override
    public boolean isNull() {
      return accessor.isNull(rowIndex());
    }

    <@get drillType accessorType label />
  }

  public static class ${drillType}ColumnWriter extends AbstractColumnWriter {

    <@bindWriter "" drillType />

    <@getType label />

    <@set drillType accessorType label false />
  }

  public static class Nullable${drillType}ColumnWriter extends AbstractColumnWriter {

    <@bindWriter "Nullable" drillType />

    <@getType label />

    @Override
    public void setNull() {
      mutator.setNull(rowIndex());
    }

    <@set drillType accessorType label true />
  }

    </#if>
  </#list>
</#list>

  public static void defineReaders(
      Class<? extends AbstractColumnReader> readers[][]) {
<#list vv.types as type>
  <#list type.minor as minor>
    <#assign drillType=minor.class>
    <#assign notyet=minor.accessorDisabled!type.accessorDisabled!false>
    <#if ! notyet>
    <#assign typeEnum=drillType?upper_case>
    readers[MinorType.${typeEnum}.ordinal()][DataMode.REQUIRED.ordinal()] = ${drillType}ColumnReader.class;
    readers[MinorType.${typeEnum}.ordinal()][DataMode.OPTIONAL.ordinal()] = Nullable${drillType}ColumnReader.class;
    </#if>
  </#list>
</#list>
  }

  public static void defineWriters(
      Class<? extends AbstractColumnWriter> writers[][]) {
<#list vv.types as type>
<#list type.minor as minor>
  <#assign drillType=minor.class>
  <#assign notyet=minor.accessorDisabled!type.accessorDisabled!false>
  <#if ! notyet>
    <#assign typeEnum=drillType?upper_case>
    writers[MinorType.${typeEnum}.ordinal()][DataMode.REQUIRED.ordinal()] = ${drillType}ColumnWriter.class;
    writers[MinorType.${typeEnum}.ordinal()][DataMode.OPTIONAL.ordinal()] = Nullable${drillType}ColumnWriter.class;
  </#if>
</#list>
</#list>
  }
}
