/**
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

package org.apache.drill.exec.server.options;

import org.apache.drill.exec.server.options.OptionValue.Kind;

/**
 * A basic implementation of an {@link OptionsService}.
 */
public abstract class BaseOptionSet implements OptionsService {
  /**
   * Gets the current option value given a validator.
   *
   * @param validator the validator
   * @return option value
   * @throws IllegalArgumentException - if the validator is not found
   */
  private OptionValue getOptionSafe(OptionValidator validator) {
    final String optionName = validator.getOptionName();
    OptionValue value = getOption(optionName);
    return value == null ? getDefault(optionName) : value;
  }

  @Override
  public boolean getOption(TypeValidators.BooleanValidator validator) {
    return getOptionSafe(validator).bool_val;
  }

  @Override
  public double getOption(TypeValidators.DoubleValidator validator) {
    return getOptionSafe(validator).float_val;
  }

  @Override
  public long getOption(TypeValidators.LongValidator validator) {
    return getOptionSafe(validator).num_val;
  }

  @Override
  public String getOption(TypeValidators.StringValidator validator) {
    return getOptionSafe(validator).string_val;
  }

  @Override
  public boolean getBoolean(String name) {
    return getByType(name, Kind.BOOLEAN).bool_val;
  }

  @Override
  public long getLong(String name) {
    return getByType(name, Kind.LONG).num_val;
  }

  @Override
  public double getDouble(String name) {
    return getByType(name, Kind.DOUBLE).float_val;
  }

  @Override
  public String getString(String name) {
    return getByType(name, Kind.STRING).string_val;
  }

  private OptionValue getByType(String name, Kind dataType) {
    OptionValue value = getOption(name);
    if (value == null) {
      throw new IllegalArgumentException("Undefined option: " + name);
    }
    if (value.kind != dataType) {
      throw new IllegalArgumentException("Option " + name + " is of data type " +
                                         value.kind + " but was requested as " + dataType);
    }
    return value;
  }
}
