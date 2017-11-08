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
package org.apache.drill.exec.physical.rowSet.model.single;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.model.MetadataProvider;
import org.apache.drill.exec.physical.rowSet.model.MetadataProvider.MetadataCreator;
import org.apache.drill.exec.physical.rowSet.model.MetadataProvider.VectorDescrip;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.reader.AbstractObjectReader;
import org.apache.drill.exec.vector.accessor.reader.MapReader;
import org.apache.drill.exec.vector.accessor.reader.ObjectArrayReader;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;

/**
 * Builds an implicit join reader given a container (which provides the
 * schema) and an access path which consists of:
 * <ul>
 * <li>An implicit name for the top-level table.</li>
 * <li>One or more references to columns which are either a map
 * or a repeated map.</li>
 * </ul>
 * Throws an error if the names do not match fields or match
 * non-map fields.
 */

public class JoinReaderBuilder extends BaseReaderBuilder {

  private final VectorContainer container;
  private final ImplicitJoinReaderImpl.ImplicitTableImpl tables[];

  /**
   * Builder for an implicit join reader.
   *
   * @param container the container that has the data to read
   *
   * @param tableNames tables. The first is the implicit name for the
   * root tuple, others must match maps (single or array) at each level
   * in the column tree
   */

  public JoinReaderBuilder(VectorContainer container, List<String> tableNames) {
    this.container = container;
    tables = buildTables(tableNames);
  }

  private static ImplicitJoinReaderImpl.ImplicitTableImpl[] buildTables(List<String> tableNames) {
    if (tableNames == null || tableNames.size() < 2) {
      throw new IllegalStateException("Implicit Join Reader requires a table list");
    }

    ImplicitJoinReaderImpl.ImplicitTableImpl tables[] = new ImplicitJoinReaderImpl.ImplicitTableImpl[tableNames.size()];
    for (int i = 0; i < tables.length; i++) {
      tables[i] = new ImplicitJoinReaderImpl.ImplicitTableImpl(i, tableNames.get(i));
    }
    for (int i = 0; i < tables.length - 1; i++) {
      tables[i].child = tables[i+1];
    }
    return tables;
  }

  public ImplicitJoinReader build() {
    return buildContainerChildren(new MetadataCreator());
  }

  public ImplicitJoinReader buildContainerChildren(MetadataProvider mdProvider) {
    List<AbstractObjectReader> readers = buildContainerChildren(container, mdProvider);
    tables[0].reader = MapReader.build(mdProvider.tuple(), readers);
    tables[0].rowCount = container.getRecordCount();
    tables[0].schema = mdProvider.tuple();
    return new ImplicitJoinReaderImpl(tables);
  }

  @Override
  protected List<AbstractObjectReader> buildContainerChildren(
      VectorContainer container, MetadataProvider mdProvider) {
    ImplicitJoinReaderImpl.ImplicitTableImpl targetTable = tables[1];
    List<AbstractObjectReader> readers = new ArrayList<>();
    for (int i = 0; i < container.getNumberOfColumns(); i++) {
      @SuppressWarnings("resource")
      ValueVector vector = container.getValueVector(i).getValueVector();
      if (vector.getField().getName().equalsIgnoreCase(targetTable.name)) {
        buildTable(vector, targetTable);
      } else {
        VectorDescrip descrip = new VectorDescrip(mdProvider, i, vector.getField());
        readers.add(buildVectorReader(vector, descrip));
      }
    }
    validateTable(targetTable);
    return readers;
  }

  private void buildTable(ValueVector vector,
      ImplicitJoinReaderImpl.ImplicitTableImpl implicitTable) {
    MajorType type = vector.getField().getType();
    if (type.getMinorType() != MinorType.MAP) {
      throw new IllegalStateException(
          String.format("Table path matched a primitive at level %d, name = `%s`",
               implicitTable.level, vector.getField().getName()));
    }
    AbstractMapVector mapVector = (AbstractMapVector) vector;
    MetadataProvider mdProvider = new MetadataCreator();
    List<AbstractObjectReader> readers;
    if (implicitTable.child == null) {
      readers = buildMap(mapVector, mdProvider);
    } else {
      readers = buildMap(mapVector, mdProvider, implicitTable.child);
    }
    AbstractObjectReader mapReader = MapReader.build(
        mdProvider.tuple(), readers);
    if (type.getMode() == DataMode.REPEATED) {
      implicitTable.reader = ObjectArrayReader.build((RepeatedMapVector) vector, mapReader);
    } else {
      implicitTable.reader = mapReader;
    }
    implicitTable.schema = mdProvider.tuple();
    implicitTable.rowCount = vector.getAccessor().getValueCount();
  }

  private List<AbstractObjectReader> buildMap(AbstractMapVector mapVector, MetadataProvider provider, ImplicitJoinReaderImpl.ImplicitTableImpl targetTable) {
    List<AbstractObjectReader> readers = new ArrayList<>();
    int i = 0;
    for (ValueVector child : mapVector) {
      if (child.getField().getName().equalsIgnoreCase(targetTable.name)) {
        buildTable(child, targetTable);
      } else {
        VectorDescrip descrip = new VectorDescrip(provider, i, child.getField());
        readers.add(buildVectorReader(child, descrip));
      }
      i++;
    }
    validateTable(targetTable);
    return readers;
  }

  private void validateTable(ImplicitJoinReaderImpl.ImplicitTableImpl implicitTable) {
    if (implicitTable == null   ||  implicitTable.reader != null) {
      return;
    }
    throw new IllegalStateException(
        String.format("No match found for table `%s`  at level ",
            implicitTable.name, implicitTable.level));
  }
}
