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
package org.apache.drill.exec.physical.impl.scan.project.projSet;

import org.apache.drill.exec.record.metadata.ColumnMetadata;

public class WildcardAndTransformProjectionSet extends AbstractProjectionSet {

  private final CustomTypeTransform customTransform;

  public WildcardAndTransformProjectionSet(CustomTypeTransform customTransform) {
    this.customTransform = customTransform;
  }

  @Override
  public ColumnReadProjection readProjection(ColumnMetadata col) {
    if (isSpecial(col)) {
      return new UnprojectedReadColProj(col);
    }

    return new TransformReadColProj(col, customTransform);
  }
}
