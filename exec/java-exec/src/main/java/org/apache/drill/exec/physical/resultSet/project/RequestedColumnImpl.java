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
package org.apache.drill.exec.physical.resultSet.project;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.resultSet.project.Qualifier.MapQualifier;
import org.apache.drill.exec.physical.resultSet.project.Qualifier.SelectedArrayQualifier;
import org.apache.drill.exec.record.metadata.ColumnMetadata;

/**
 * Represents one name element. Like a {@link NameSegment}, except that this
 * version is an aggregate. If the projection list contains `a.b` and `a.c`,
 * then one name segment exists for a, and contains segments for both b and c.
 */
public class RequestedColumnImpl implements RequestedColumn {

  private final RequestedTuple parent;
  private final String name;
  private List<Qualifier> qualifiers;

  public RequestedColumnImpl(RequestedTuple parent, String name) {
    this.parent = parent;
    this.name = name;
  }

  @Override
  public String name() { return name; }

  @Override
  public boolean isSimple() { return qualifiers == null; }

  protected Qualifier qualifier(int posn) {
    if (qualifiers == null || qualifiers.size() <= posn) {
      return null;
    }
    return qualifiers.get(posn);
  }

  @Override
  public RequestedColumn.PathType pathType(int posn) {
    Qualifier qual = qualifier(posn);
    return qual == null ? null : qual.pathType();
  }

  public void setQualifier(int posn, Qualifier qualifier) {
    if (qualifiers == null) {
      qualifiers = new ArrayList<>();
    }
    if (posn == qualifiers.size()) {
      qualifiers.add(qualifier);
    } else {
      qualifiers.set(posn, qualifier);
    }
  }

  @Override
  public String fullName() {
    final StringBuilder buf = new StringBuilder();
    buildName(buf);
    return buf.toString();
  }

  public boolean isRoot() { return parent == null; }

  @Override
  public boolean isTuple() {
    if (qualifiers == null && qualifiers.isEmpty()) {
      return false;
    }
    return pathType(qualifiers.size() - 1) == RequestedColumn.PathType.MAP;
  }

  @Override
  public boolean isDict() {
    return pathType(0) == RequestedColumn.PathType.KEY;
  }

  @Override
  public boolean isArray() {
    return pathType(0) == RequestedColumn.PathType.ARRAY;
  }

  @Override
  public boolean hasIndexes() {
    Qualifier qual = qualifier(0);
    return qual != null && qual instanceof SelectedArrayQualifier;
  }

  @Override
  public boolean hasIndex(int index) {
    if (!hasIndexes()) {
      return false;
    }
    return ((SelectedArrayQualifier) qualifier(0)).hasIndex(index);
  }

  @Override
  public int maxIndex() {
    if (!hasIndexes()) {
      return 0;
    }
    return ((SelectedArrayQualifier) qualifier(0)).maxIndex();
  }

  @Override
  public boolean[] indexes() {
    if (!hasIndexes()) {
      return null;
    }
    return ((SelectedArrayQualifier) qualifier(0)).indexArray();
  }

  @Override
  public RequestedTuple mapProjection() {
    if (qualifiers == null || qualifiers.isEmpty()) {
      return ImpliedTupleRequest.ALL_MEMBERS;
    }
    Qualifier qual = qualifiers.get(qualifiers.size() - 1);
    if (qual instanceof MapQualifier) {
      return ((MapQualifier) qual).members();
    } else {
      return ImpliedTupleRequest.ALL_MEMBERS;
    }
  }

  protected void buildName(StringBuilder buf) {
    parent.buildName(buf);
    buf.append('`')
       .append(name)
       .append('`');
  }

  @Override
  public boolean nameEquals(String target) {
    return name.equalsIgnoreCase(target);
  }

  /**
   * Convert the projection to a string of the form:
   * {@code a[0,1,4]['*']{b, c d}}.
   * The information here s insufficient to specify a type,
   * it only specifies a pattern to which types are compatible.
   */
  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder()
        .append(name);
    if (qualifiers != null) {
      for (Qualifier qualifier : qualifiers) {
        buf.append(qualifier.toString());
      }
    }
    return buf.toString();
  }

  /**
   * Reports if the projection (representing an item in a projection list)
   * is compatible with an actual schema column.
   *
   * @param majorType type of a schema column
   * @return true if this projection type is compatible with the
   * column's projection type
   */
  @Override
  public boolean isConsistentWith(ColumnMetadata col) {
    return checkPath(0, col) != NOT_COMPATIBLE;
  }

  private int checkPath(int level, ColumnMetadata col) {
    MajorType type = col.majorType();
    if (isArrayLike(type) && pathType(level) == RequestedColumn.PathType.ARRAY) {
      level++;
    }
    switch (col.type()) {
    case DICT:
      level = checkDict(level, type);
      if (level > 0) {
        // TODO
//        level = checkPath(level, col.valueCol());
      }
      return level;
    case LIST:
      return checkList(level, type);
    case MAP:
      return checkMap(level, type);
    case UNION:
      return checkUnion(level, type);
    case NULL: // What are rules for NULL?
    default:
      return checkScalar(level, type);
    }
  }

  public static final int ACCEPT_ALL = -1;
  public static final int NOT_COMPATIBLE = -2;

  /**
   * Reports if the projection (representing an item in a projection list)
   * is compatible with an actual schema column. This form is limited to
   * what the major type can provide: checks are done only at the first
   * level. Specifically:
   *
   * <ul>
   * <li>MAP: Does not check map members.</li>
   * <li>DICT: Does check value type.</li>
   * <li>LIST, UNION: Does not check members.</li>
   * </ul>
   *
   * @param majorType type of a schema column
   * @return true if this projection type is compatible with the
   * column's projection type
   */
  @Override
  public boolean isConsistentWith(MajorType type) {
    int result = checkPath(0, type);
    return result != NOT_COMPATIBLE;
  }

  private static boolean isArrayLike(MajorType type) {
    return Types.isRepeated(type) || type.getMinorType() == MinorType.LIST;
  }

  private int checkPath(int level, MajorType type) {
    if (isArrayLike(type) && pathType(level) == RequestedColumn.PathType.ARRAY) {
      level++;
    }
    switch (type.getMinorType()) {
    case DICT:

      // Recursively check the value type, if provided.
      //return checkPath(level, Types.getValueType(type));
      return checkDict(level, type);
    case LIST:
      return checkList(level, type);
    case MAP:
      return checkMap(level, type);
    case UNION:
      return checkUnion(level, type);
    case NULL: // What are rules for NULL?
    default:
      return checkScalar(level, type);
    }
  }

  private int checkScalar(int level, MajorType type) {
    return pathType(level) == null ? ACCEPT_ALL : NOT_COMPATIBLE;
  }

  private int checkMap(int level, MajorType type) {
    RequestedColumn.PathType pathType = pathType(level);
    if (pathType == null) {
      return ACCEPT_ALL;
    }
    if (pathType != RequestedColumn.PathType.MAP) {
      return NOT_COMPATIBLE;
    }
    return level;
  }

  private int checkDict(int level, MajorType type) {
    RequestedColumn.PathType pathType = pathType(level);
    if (pathType == null) {
      return ACCEPT_ALL;
    }
    if (pathType != RequestedColumn.PathType.KEY) {
      return NOT_COMPATIBLE;
    }
    return level;
  }

  /**
   * A LIST is an array, and can be repeated for two levels of
   * array. However, the LIST members can be a UNION, which can be
   * anything. So, go through the formalities of checking, but we
   * end up accepting everything.
   */
  private int checkList(int level, MajorType type) {
    // Contents can be a UNION, so just punt. The major type, and
    // the UNION type are both under-constrained; we don't know what
    // they contain.
    return ACCEPT_ALL;
  }

  /**
   * A UNION can contain anything at run time. Very hard to check
   * consistency. For now, just punt.
   */
  private int checkUnion(int level, MajorType type) {
    return ACCEPT_ALL;
  }
}
