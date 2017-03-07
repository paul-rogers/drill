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
package org.apache.drill.exec.vector.accessor;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;

public class ColumnAccessorFactory {

  private static Class<? extends AbstractColumnWriter> writers[][] = buildWriters();
  private static Class<? extends AbstractColumnReader> readers[][] = buildReaders();

  @SuppressWarnings("unchecked")
  private static Class<? extends AbstractColumnWriter>[][] buildWriters() {
    int typeCount = MinorType.values().length;
    int modeCount = DataMode.values().length;
    Class<? extends AbstractColumnWriter> writers[][] = new Class[typeCount][];
    for (int i = 0; i < typeCount; i++) {
      writers[i] = new Class[modeCount];
    }

    ColumnAccessors.defineWriters(writers);
    return writers;
  }

  @SuppressWarnings("unchecked")
  private static Class<? extends AbstractColumnReader>[][] buildReaders() {
    int typeCount = MinorType.values().length;
    int modeCount = DataMode.values().length;
    Class<? extends AbstractColumnReader> readers[][] = new Class[typeCount][];
    for (int i = 0; i < typeCount; i++) {
      readers[i] = new Class[modeCount];
    }

    ColumnAccessors.defineReaders(readers);
    return readers;
  }

  public static AbstractColumnWriter newWriter(MajorType type) {
    Class<? extends AbstractColumnWriter> writerClass = writers[type.getMinorType().ordinal()][type.getMode().ordinal()];
    if (writerClass == null) {
      throw new UnsupportedOperationException();
    }
    try {
      return writerClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  public static AbstractColumnReader newReader(MajorType type) {
    Class<? extends AbstractColumnReader> writerClass = readers[type.getMinorType().ordinal()][type.getMode().ordinal()];
    if (writerClass == null) {
      throw new UnsupportedOperationException();
    }
    try {
      return writerClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }
}