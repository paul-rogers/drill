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
package org.apache.drill.exec.store.easy.json.loader.mongo;

import org.apache.drill.exec.store.easy.json.loader.BaseFieldFactory;
import org.apache.drill.exec.store.easy.json.loader.FieldFactory;
import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ObjectValueListener;
import org.apache.drill.exec.store.easy.json.loader.TupleListener;
import org.apache.drill.exec.store.easy.json.loader.mongo.ExtendedTypeObjectListener.ExtendedTypeFieldListener;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.exec.store.easy.json.parser.ObjectListener.FieldType;
import org.apache.drill.exec.store.easy.json.parser.ValueDef.JsonType;

public class ExtendedTypeFieldFactory extends BaseFieldFactory {

  public ExtendedTypeFieldFactory(TupleListener tupleListener, FieldFactory child) {
    super(tupleListener, child);
  }

  @Override
  public FieldType fieldType(String key) {
    return child.fieldType(key);
  }

  @Override
  public ValueListener addField(String key, ValueDef valueDef) {
    // TODO Auto-generated method stub
    return null;
  }

  public ObjectValueListener becomeMap(String key) {
    return objectListenerFor(key);
  }

  public boolean isExtendedTypeName(String key) {
    // TODO: If this is a performance issue, replace with a
    // static map of contructors.
    switch (key) {
      case ExtendedTypeNames.LONG:
        return true;
      default:
        return false;
    }
  }

  public ValueListener extendedTypeListenerFor(String key) {
    return new ExtendedTypeFieldListener(
        extendedObjectListenerFor(key));
  }

  private ExtendedTypeObjectListener extendedObjectListenerFor(String key) {
    // TODO: If this is a performance issue, replace with a
    // static map of contructors.
    switch (key) {
      case ExtendedTypeNames.LONG:
        return new Int64ObjectListener(tupleListener, key);
      default:
        throw new IllegalStateException("Unexpected extended type: " + key);
    }
  }
}
