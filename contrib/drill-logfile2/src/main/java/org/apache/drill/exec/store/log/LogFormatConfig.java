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
package org.apache.drill.exec.store.log;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Objects;
import org.apache.drill.common.logical.FormatPluginConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@JsonTypeName("log")
public class LogFormatConfig implements FormatPluginConfig {

  public String regex;
  public List<String> fields = new ArrayList<>();
  public String extension;
  public List<String> dataTypes = new ArrayList<>();
  public boolean errorOnMismatch;

  public String getRegex() { return regex; }
  public List<String> getFields() { return fields; }
  public String getExtension() { return extension; }
  public List<String> getDataTypes() { return dataTypes; }
  public boolean getErrorOnMismatch() { return errorOnMismatch;}

  @Override
  public boolean equals(Object obj) {
    if (this == obj) { return true; }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    LogFormatConfig other = (LogFormatConfig) obj;
    return Objects.equal(regex, other.regex) &&
        Objects.equal(fields, other.fields) &&
        Objects.equal(dataTypes, other.dataTypes) &&
        Objects.equal(errorOnMismatch, other.errorOnMismatch) &&
        Objects.equal(extension, other.extension);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(new Object[] {regex, fields, extension});
  }
}