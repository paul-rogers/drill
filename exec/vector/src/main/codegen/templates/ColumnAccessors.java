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
    public ValueType valueType() {
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
      accessor = ((${prefix}${drillType}Vector) vector).getAccessor();
    }

  <#if drillType = "Decimal9" || drillType == "Decimal18">
    @Override
    public void bind(RowIndex rowIndex, MaterializedField field, VectorAccessor va) {
      bind(rowIndex, field, va);
      this.field = field;
    }

 </#if>
   private ${prefix}${drillType}Vector.Accessor accessor() {
      if (vectorAccessor == null) {
        return accessor;
      } else {
        return ((${prefix}${drillType}Vector) vectorAccessor.vector()).getAccessor();
      }
    }
</#macro>
<#macro get drillType accessorType label>
    @Override
    public ${accessorType} get${label}() {
  <#if drillType == "VarChar">
      return new String(accessor().get(rowIndex.index()), Charsets.UTF_8);
  <#elseif drillType == "Var16Char">
      return new String(accessor().get(rowIndex.index()), Charsets.UTF_16);
  <#elseif drillType == "VarBinary">
      return accessor().get(rowIndex.index());
  <#elseif drillType == "Decimal9" || drillType == "Decimal18">
      return DecimalUtility.getBigDecimalFromPrimitiveTypes(
                accessor().get(rowIndex.index()),
                field.getScale(),
                field.getPrecision());
  <#elseif accessorType == "Decimal18">
      return DecimalUtilities.getBigDecimalFromPrimitiveTypes(accessor().getObject(rowIndex.index());
  <#elseif accessorType == "BigDecimal" || accessorType == "Period">
      return accessor().getObject(rowIndex.index());
  <#else>
      return accessor().get(rowIndex.index());
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
      mutator.setSafe(rowIndex.index(), bytes, 0, bytes.length);
  <#elseif drillType == "Var16Char">
      byte bytes[] = value.getBytes(Charsets.UTF_16);
      mutator.setSafe(rowIndex.index(), bytes, 0, bytes.length);
  <#elseif drillType == "VarBinary">
      mutator.setSafe(rowIndex.index(), value, 0, value.length);
  <#elseif drillType == "Decimal9">
      mutator.setSafe(rowIndex.index(),
          DecimalUtility.getDecimal9FromBigDecimal(value,
              field.getScale(), field.getPrecision()));
  <#elseif drillType == "Decimal18">
      mutator.setSafe(rowIndex.index(),
          DecimalUtility.getDecimal18FromBigDecimal(value,
              field.getScale(), field.getPrecision()));
  <#elseif drillType == "IntervalYear">
      mutator.setSafe(rowIndex.index(), value.getYears() * 12 + value.getMonths());
  <#elseif drillType == "IntervalDay">
      mutator.setSafe(rowIndex.index(),<#if nullable> 1,</#if>
                      value.getDays(),
                      ((value.getHours() * 60 + value.getMinutes()) * 60 +
                       value.getSeconds()) * 1000 + value.getMillis());
  <#elseif drillType == "Interval">
      mutator.setSafe(rowIndex.index(),<#if nullable> 1,</#if>
                      value.getYears() * 12 + value.getMonths(),
                      value.getDays(),
                      ((value.getHours() * 60 + value.getMinutes()) * 60 +
                       value.getSeconds()) * 1000 + value.getMillis());
  <#else>
      mutator.setSafe(rowIndex.index(), <#if cast=="set">(${javaType}) </#if>value);
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
import org.joda.time.Period;

/**
 * Basic accessors for most Drill vector types and modes. These are bare-bones
 * accessors: they do only the most rudimentary type conversions. For all,
 * there is only one way to get/set values; they don't convert from, say,
 * a double to an int or visa-versa.
 * <p>
 * Writers work only with single vectors. Readers work with either single
 * vectors or a "hyper vector": a collection of vectors indexed together.
 * The details are hidden behind the {@link RowIndex} interface. If the reader
 * accesses a single vector, then the mutator is cached at bind time. However,
 * if the reader works with a hyper vector, then the vector is null at bind
 * time and must be retrieved for each row (since the vector differs row-by-
 * row.)
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
      return accessor().isNull(rowIndex.index());
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
      mutator.setNull(rowIndex.index());
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
