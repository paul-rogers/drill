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

import java.util.Collection;
import java.util.List;

import org.apache.calcite.util.Pair;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.resultSet.project.Qualifier.FullArrayQualifier;
import org.apache.drill.exec.physical.resultSet.project.Qualifier.MapQualifier;
import org.apache.drill.exec.physical.resultSet.project.Qualifier.SelectedArrayQualifier;
import org.apache.drill.exec.physical.resultSet.project.RequestedTuple.TupleProjectionType;
import org.apache.drill.common.expression.PathSegment.ArraySegment;
import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts a projection list passed to an operator into a scan projection list,
 * coalescing multiple references to the same column into a single reference.
 */
public class Projections {
  private static final Logger logger = LoggerFactory.getLogger(Projections.class);
  private static final FullArrayQualifier ARRAY_WILDCARD_QUALIFIER = new FullArrayQualifier();
  private static final MapQualifier PROJECT_ALL_MEMBERS = new MapQualifier(projectAll());

  private static abstract class TupleHolder {

    protected abstract void becomeWildcard();

    protected abstract Pair<RequestedColumn, Boolean> addMember(String colName);

    protected Pair<RequestedColumn, Boolean> addMember(RequestedTupleImpl tuple, String colName) {
      RequestedColumn member = tuple.get(colName);
      boolean isNew = member == null;

      // Simplest case: never saw this column before. Assume it is a simple
      // projection: a

      if (isNew) {
        if (colName.equals(SchemaPath.DYNAMIC_STAR)) {
          member = new RequestedWildcardColumn(tuple, colName);
          tuple.projectionType = TupleProjectionType.ALL;
        } else {
          member = new RequestedColumnImpl(tuple, colName);
        }
        tuple.add(member);
      }
      return Pair.of(member, isNew);
    }
  }

  private static class RowHolder extends TupleHolder {
    private final RequestedTupleImpl tupleProj;

    public RowHolder(RequestedTupleImpl tupleProj) {
      this.tupleProj = tupleProj;
    }

    /**
     * Top-level columns cannot include a wildcard.
     */
    @Override
    public void becomeWildcard() {
      throw new IllegalArgumentException("Wildcard must appear by itself in the projection list");
    }

    @Override
    public Pair<RequestedColumn, Boolean> addMember(String colName) {
      return addMember(tupleProj, colName);
    }
  }

  private static class MapHolder extends TupleHolder {
    private final RequestedColumnImpl col;
    private final int mapLevel;

    public MapHolder(RequestedColumnImpl col, int mapLevel) {
      this.col = col;
      this.mapLevel = mapLevel;
    }

    @Override
    public void becomeWildcard() {
      col.setQualifier(mapLevel, PROJECT_ALL_MEMBERS);
    }

    @Override
    public Pair<RequestedColumn, Boolean> addMember(String colName) {
      MapQualifier qual = (MapQualifier) col.qualifier(mapLevel);
      RequestedTuple members = qual.members();
      if (members.type() != TupleProjectionType.SOME) {
        // m.* & m.a --> m.*
        return null;
      } else if (colName.equals(SchemaPath.DYNAMIC_STAR)) {
        // m.a & m.* --> m.*
        col.setQualifier(mapLevel, PROJECT_ALL_MEMBERS);
        return null;
      } else  {
        // m.a && m.b --> m.{a, b}
        return addMember((RequestedTupleImpl) members, colName);
      }
    }
  }

  private Projections() { }

  public static RequestedTuple projectAll() {
    return ImpliedTupleRequest.ALL_MEMBERS;
  }

  public static RequestedTuple projectNone() {
    return ImpliedTupleRequest.NO_MEMBERS;
  }

  /**
   * Parse a projection list. The list should consist of a list of column names;
   * or wildcards. An empty list means
   * nothing is projected. A null list means everything is projected (that is, a
   * null list here is equivalent to a wildcard in the SELECT statement.)
   * <p>
   * The projection list may include both a wildcard and column names (as in
   * the case of implicit columns.) This results in a final list that both
   * says that everything is projected, and provides the list of columns.
   * <p>
   * Parsing is used at two different times. First, to parse the list from
   * the physical operator. This has the case above: an explicit wildcard
   * and/or additional columns. Then, this class is used again to prepare the
   * physical projection used when reading. In this case, wildcards should
   * be removed, implicit columns pulled out, and just the list of read-level
   * columns should remain.
   *
   * @param projList
   *          the list of projected columns, or null if no projection is to be
   *          done
   * @return a projection set that implements the specified projection
   */
  public static RequestedTuple parse(Collection<SchemaPath> projList) {
    if (projList == null) {
      return projectAll();
    }
    if (projList.isEmpty()) {
      return projectNone();
    }
    RequestedTupleImpl tupleProj = new RequestedTupleImpl();
    TupleHolder tupleHolder = new RowHolder(tupleProj);
    for (SchemaPath col : projList) {
      parseMember(tupleHolder, col.getRootSegment());
    }
    return tupleProj;
  }

  private static void parseMember(TupleHolder tupleHolder, NameSegment nameSeg) {
    String colName = nameSeg.getPath();
    Pair<RequestedColumn, Boolean> result = tupleHolder.addMember(colName);

    // If column is known, drill down further.
    if (result != null) {
      RequestedColumn member = result.left;
      boolean isNew = result.right;
      if (member.isWildcard()) {
        if (!isNew) {
          throw UserException
            .validationError()
            .message("Duplicate wildcard in project list")
            .build(logger);
        }
      } else {
        parsePath((RequestedColumnImpl) member, isNew, 0, nameSeg);
      }
    }
  }

  private static void parsePath(RequestedColumnImpl col, boolean isNew, int level, PathSegment parentPath) {
    if (parentPath.isLastPath()) {
      parseLeaf(col, isNew, level);
    } else {
      PathSegment seg = parentPath.getChild();
      if (seg.isArray()) {
        parseArray(col, isNew, level, (ArraySegment) seg);
      } else {
        parseMember(col, isNew, level, (NameSegment) seg);
      }
    }
  }

  /**
   * Parse a projection of the form {@code a}: that is, just a bare column.
   */
  private static void parseLeaf(RequestedColumnImpl col, boolean isNew, int level) {
    // We already have a column. Coalesce this (simple) projection with the
    // existing one.

    // If there is an existing simple column, then we have
    // SELECT a, a
    // Which is not valid at this level.

    Qualifier qualifier = col.qualifier(level);
    if (qualifier == null) {
      if (isNew) {
        // New simple column.
        return;
      } else {
        // Saw a and a. Not supported at the scan level; the planner should have
        // removed duplicates in the SELECT a AS X, a AS Y case.
        throw UserException
          .validationError()
          .message("Duplicate column in project list: %s",
              col.fullName())
          .build(logger);
      }
    }

    switch (qualifier.pathType()) {
    case ARRAY:
      // a && a[0] --> a[*]
      // Saw both a and a[x]. Occurs in project list.
      // Project all elements.
      if (qualifier instanceof SelectedArrayQualifier) {
        col.setQualifier(level, ARRAY_WILDCARD_QUALIFIER);
      }
      break;
    case MAP:
      // a && a.b --> a.*
      // Allow both a.b (existing) and a (this column)
      // Since we we know a is a map, and we've projected the
      // whole map, modify the projection of the column to
      // project the entire map.
      MapQualifier mapQualifier = (MapQualifier) qualifier;
      if (mapQualifier.members().type() != TupleProjectionType.ALL) {
        col.setQualifier(level, PROJECT_ALL_MEMBERS);
      }
      break;
    default:
      assert false;
    }
  }

  private static void parseArray(RequestedColumnImpl col, boolean isNew, int level, ArraySegment arraySeg) {
    parseArraySeg(col, isNew, level, arraySeg);
    if (!arraySeg.isLastPath()) {
      parsePath(col, isNew, level + 1, arraySeg);
    }
  }

  private static void parseArraySeg(RequestedColumnImpl col, boolean isNew, int level, ArraySegment arraySeg) {
    Qualifier qualifier = col.qualifier(level);
    if (qualifier == null) {
      if (!isNew) {
        // a[n] && a --> a[*]
        // Previous projection was simple, this projection is a
        // specific array element, so project all array elements.
        col.setQualifier(level, ARRAY_WILDCARD_QUALIFIER);
      } else if (level > 0) {
        // a[?][n] --> a[?][*]
        // We do not read-time filtering on second or deeper levels
        // of indexes; project all elements, simply recording that
        // this must be an n-dimensional array.
        col.setQualifier(level, ARRAY_WILDCARD_QUALIFIER);
      } else {
        // Saw a[x].
        SelectedArrayQualifier arrayQual = new SelectedArrayQualifier();
        arrayQual.indexes().add(arraySeg.getIndex());
        col.setQualifier(level, arrayQual);
       }
      return;
    }

    // An existing column already has a qualifier. Must be an
    // array qualifier.

    switch (qualifier.pathType()) {
    case ARRAY:
      if (qualifier instanceof SelectedArrayQualifier) {
        ((SelectedArrayQualifier) qualifier).indexes().add(arraySeg.getIndex());
      }
      break;
    case MAP:
      throw UserException
        .validationError()
        .message("Cannot project a column as both %s[n] and %s.member",
            col.fullName(), col.fullName())
        .build(logger);
    default:
      assert false;
    }
  }

  private static void parseMember(RequestedColumnImpl col, boolean isNew, int level, NameSegment memberSeg) {
    String childName = memberSeg.getPath();
    Qualifier qualifier = col.qualifier(level);
    if (qualifier == null) {
      // Column is simple projection; add map projection
      if (isNew) {
        if (childName.equals(SchemaPath.DYNAMIC_STAR)) {
          // Saw m.*
          col.setQualifier(level, PROJECT_ALL_MEMBERS);
        } else {
          // Saw m.c
          MapQualifier mapQual = new MapQualifier(new RequestedTupleImpl(col));
          col.setQualifier(level, mapQual);
          parseMember(new MapHolder(col, level), memberSeg);
        }
      } else {
        // a.b && a --> a.*
        col.setQualifier(level, PROJECT_ALL_MEMBERS);
      }
      return;
    }

    // Column has a qualifier. Must be a map qualifier.
    switch (qualifier.pathType()) {
    case ARRAY:
      throw UserException
        .validationError()
        .message("Cannot project a column as both %s.member and %s[n]",
            col.fullName(), col.fullName())
        .build(logger);
    case MAP:
      parseMember(new MapHolder(col, level), memberSeg);
      break;
    default:
      assert false;
    }
  }

  /**
   * Create a requested tuple projection from a rewritten top-level
   * projection list. The columns within the list have already been parsed to
   * pick out arrays, maps and scalars. The list must not include the
   * wildcard: a wildcard list must be passed in as a null list. An
   * empty list means project nothing. Null list means project all, else
   * project only the columns in the list.
   *
   * @param projList top-level, parsed columns
   * @return the tuple projection for the top-level row
   */
  public static RequestedTuple build(List<RequestedColumn> projList) {
    if (projList == null) {
      return new ImpliedTupleRequest(true);
    }
    if (projList.isEmpty()) {
      return ImpliedTupleRequest.NO_MEMBERS;
    }
    return new RequestedTupleImpl(projList);
  }
}
