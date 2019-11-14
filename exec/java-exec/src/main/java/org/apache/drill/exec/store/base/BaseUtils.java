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
package org.apache.drill.exec.store.base;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.commons.lang.reflect.ConstructorUtils;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseUtils {

  static final Logger logger = LoggerFactory.getLogger(BaseUtils.class);

  private BaseUtils() { }

  /**
   * Given a storage plugin registry and a storage plugin config, look
   * up the storage plugin. Handles errors by converting them to Drill's
   * usual {@link UserException} form.
   */
  public static BaseStoragePlugin<?> resolvePlugin(StoragePluginRegistry engineRegistry,
      StoragePluginConfig config) {
    try {
      StoragePlugin plugin = engineRegistry.getPlugin(config);
      if (plugin == null) {
        throw UserException.systemError(null)
          .message("Cannot find storage plugin for", config.getClass().getCanonicalName())
          .build(logger);
      }
      if (!(plugin instanceof BaseStoragePlugin)) {
        throw UserException.systemError(null)
          .message("Storage plugin %s is of wrong class: %s but should be %s",
              plugin.getName(), plugin.getClass().getCanonicalName(),
              BaseStoragePlugin.class.getSimpleName())
          .build(logger);
      }
      return (BaseStoragePlugin<?>) plugin;
    } catch (ExecutionSetupException e) {
      throw UserException.systemError(e)
        .message("Cannot find storage plugin for", config.getClass().getCanonicalName())
        .build(logger);
    }
  }

  /**
   * Create a copy of the given object by calling the copy constructor for
   * the class of that object. Used to implement copies during Calcite planning.
   * To be clear: this class calls the copy constructor of the object's own class,
   * not the copy constructor of the type parameter.
   *
   * @param <T> the type of the object (typically as a sub class of the object
   * type)
   * @param obj the object to be copied
   * @return a copy of the object
   */

  @SuppressWarnings("unchecked")
  protected static <T> T copyOf(T obj) {
    try {
      Constructor<?> ctor = obj.getClass().getConstructor(obj.getClass());
      return (T) ctor.newInstance(obj);
    } catch (NoSuchMethodException | SecurityException | InstantiationException
        | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException e) {
      throw UserException.systemError(e)
        .addContext("Likely copy constructor missing for", obj.getClass().getCanonicalName())
        .build(logger);
    }
  }

  /**
   * Copy a Calcite object with the given argument (which is typically a list of
   * columns, to create a copy with columns.
   */
  @SuppressWarnings("unchecked")
  protected static <T> T copyWith(T obj, Object arg) {
    try {
      return (T) ConstructorUtils.invokeConstructor(obj.getClass(), new Object[] {obj, arg});
    } catch (NoSuchMethodException | SecurityException | InstantiationException
        | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException e) {
      throw UserException.systemError(e)
        .addContext("Likely copy constructor missing: %s(%s, %s)",
            obj.getClass().getCanonicalName(),
            obj.getClass().getSimpleName(), arg.getClass().getSimpleName())
        .build(logger);
    }
  }
}
