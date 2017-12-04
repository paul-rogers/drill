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
package org.apache.drill.exec.physical.rowSet.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.ResultVectorCache;
import org.apache.drill.exec.physical.rowSet.impl.ColumnState.ListColumnState;
import org.apache.drill.exec.physical.rowSet.impl.SingleVectorState.FixedWidthVectorState;
import org.apache.drill.exec.physical.rowSet.impl.SingleVectorState.OffsetVectorState;
import org.apache.drill.exec.physical.rowSet.impl.UnionState.UnionVectorState;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.VariantMetadata;
import org.apache.drill.exec.record.metadata.VariantSchema;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.VariantWriter;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;
import org.apache.drill.exec.vector.accessor.writer.ListWriterImpl;
import org.apache.drill.exec.vector.accessor.writer.SimpleListShim;
import org.apache.drill.exec.vector.accessor.writer.UnionVectorShim;
import org.apache.drill.exec.vector.accessor.writer.UnionWriterImpl;
import org.apache.drill.exec.vector.complex.ListVector;
import org.apache.drill.exec.vector.complex.UnionVector;

public class ListState extends ContainerState
  implements VariantWriter.VariantWriterListener {

  protected static class ListVectorState implements VectorState {

    private final ColumnMetadata schema;
    private final ListVector vector;
    private final FixedWidthVectorState bitsVectorState;
    private final OffsetVectorState offsetVectorState;
    private VectorState memberVectorState;

    public ListVectorState(UnionWriterImpl writer, ListVector vector) {
      this.schema = writer.schema();
      this.vector = vector;
      bitsVectorState = new FixedWidthVectorState(writer, vector.getBitsVector());
      offsetVectorState = new OffsetVectorState(writer, vector.getOffsetVector(), writer.elementPosition());
      memberVectorState = new NullVectorState();
    }

    private void replaceMember(VectorState memberState) {
      memberVectorState = memberState;
    }

    private VectorState memberVectorState() { return memberVectorState; }

    @Override
    public int allocate(int cardinality) {
      return bitsVectorState.allocate(cardinality) +
          offsetVectorState.allocate(cardinality + 1) +
          memberVectorState.allocate(childCardinality(cardinality));
    }

    @Override
    public void rollover(int cardinality) {
      bitsVectorState.rollover(cardinality);
      offsetVectorState.rollover(cardinality);
      memberVectorState.rollover(childCardinality(cardinality));
    }

    private int childCardinality(int cardinality) {
      return cardinality * schema.expectedElementCount();
    }

    @Override
    public void harvestWithLookAhead() {
      bitsVectorState.harvestWithLookAhead();
      offsetVectorState.harvestWithLookAhead();
      memberVectorState.harvestWithLookAhead();
    }

    @Override
    public void startBatchWithLookAhead() {
      bitsVectorState.startBatchWithLookAhead();
      offsetVectorState.startBatchWithLookAhead();
      memberVectorState.startBatchWithLookAhead();
    }

    @Override
    public void reset() {
      bitsVectorState.reset();
      offsetVectorState.reset();
      memberVectorState.reset();
    }

    @SuppressWarnings("unchecked")
    @Override
    public ListVector vector() {
      return vector;
    }

    @Override
    public boolean isProjected() {
      return true;
    }

    @Override
    public void dump(HierarchicalFormatter format) {
      // TODO Auto-generated method stub

    }
  }

  private ListColumnState columnState;
  private final Map<MinorType, ColumnState> columns = new HashMap<>();

  public ListState(LoaderInternals loader, ResultVectorCache vectorCache,
      ProjectionSet projectionSet) {
    super(loader, vectorCache, projectionSet);
  }

  public void bindColumnState(ListColumnState columnState) {
    this.columnState = columnState;
    unionWriter().bindListener(this);
  }

  public VariantMetadata variantSchema() {
    return columnState.schema().variantSchema();
  }

  public ListWriterImpl listWriter() {
    return (ListWriterImpl) columnState.writer.array();
  }

  public UnionWriterImpl unionWriter() {
    return (UnionWriterImpl) listWriter().variant();
  }

  public ListVector listVector() {
    return columnState.vector();
  }

  public UnionVector unionVector() {
    return (UnionVector) listVectorState().memberVectorState().vector();
  }

  public ListVectorState listVectorState() {
    return (ListVectorState) columnState.vectorState();
  }

  public boolean isSingleType() {
    return ! variantSchema().expandable();
  }

  @Override
  public ObjectWriter addType(MinorType type) {
    return addMember(VariantSchema.memberMetadata(type));
  }

  @Override
  public ObjectWriter addMember(ColumnMetadata member) {
    if (variantSchema().hasType(member.type())) {
      throw new IllegalArgumentException("Duplicate type: " + member.type().toString());
    }
    if (isSingleType()) {
      throw new IllegalStateException("List is defined to contains a single type.");
    }
    return addColumn(member).writer();
  }

  /**
   * Add a new column representing a type within the list. This is where the list
   * strangeness occurs. The list starts with no type, then evolves to have a single
   * type (held by the list vector). Upon the second type, the list vector is modified
   * to hold a union vector, which then holds the existing type and the new type.
   * After that, the third and later types simply are added to the union.
   * Very, very ugly, but it is how the list vector works until we improve it...
   * <p>
   * We must make three parallel changes:
   * <ul>
   * <li>Modify the list vector structure.</li>
   * <li>Modify the union writer structure. (If a list type can evolve, then the
   * writer structure is an array of unions. But, since the union itself does
   * not exist in the 0 and 1 type cases, we use "shims" to model these odd
   * cases.</li>
   * <li>Modify the vector state for the list. If the list is "promoted" to a
   * union, then add the union to the list vector's state for management in
   * vector events.</li>
   * </ul>
   */

  @SuppressWarnings("resource")
  @Override
  protected void addColumn(ColumnState colState) {
    assert ! columns.containsKey(colState.schema().type());
    int prevColCount = columns.size();
    columns.put(colState.schema().type(), colState);

    UnionWriterImpl unionWriter = unionWriter();
    ListVector listVector = listVector();
    if (prevColCount == 0) {

      // Going from no types to one type.

      // Add the member to the list as its data vector.

      listVector.setChildVector(colState.vector());

      // Don't add the type to the vector state; we manage it as part
      // of the collection of member columns.

      // Create the single type shim.

      unionWriter.bindShim(new SimpleListShim());

    } else if (prevColCount == 1) {

      // Going from one type to a union

      // Convert the list from single type to a union,
      // moving across the previous type vector.

      ValueVector prevTypeVector = listVector.getDataVector();
      listVector.promoteToUnion();
      UnionVector unionVector = (UnionVector) listVector.getDataVector();
      unionVector.addType(prevTypeVector);

      // The union vector will be managed within the list vector state.

      listVectorState().replaceMember(new UnionVectorState(unionVector, unionWriter));

      // Replace the single-type shim with a union shim, copying
      // across the existing writer.

      SimpleListShim oldShim = (SimpleListShim) unionWriter.shim();
      UnionVectorShim newShim = new UnionVectorShim(unionVector, null);
      newShim.addMember(oldShim.memberWriter());
      unionWriter.bindShim(newShim);

    } else {

      // Already have a union; just add another type.

      unionVector().addType(colState.vector());
    }
  }

  @Override
  protected Collection<ColumnState> columnStates() {
    return columns.values();
  }

  @Override
  public int innerCardinality() {
    return columnState.outerCardinality * columnState.schema().expectedElementCount();
  }

  @Override
  protected boolean isWithinUnion() { return true; }
}
