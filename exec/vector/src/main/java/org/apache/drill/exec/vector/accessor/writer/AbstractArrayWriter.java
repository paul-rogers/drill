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
package org.apache.drill.exec.vector.accessor.writer;

import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter.ColumnWriterListener;
import org.apache.drill.exec.vector.accessor.TupleWriter.TupleWriterListener;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;

/**
 * Writer for an array-valued column. This writer appends values: once a value
 * is written, it cannot be changed. As a result, writer methods have no item
 * index; each set advances the array to the next position.
 * <p>
 * This class represents the array as a whole. In practice that means building
 * the offset vector. The array is associated with an element object that
 * manages writing to the scalar, array or tuple that is the array element. Note
 * that this representation makes little use of the methods in the "Repeated"
 * vector class: instead it works directly with the offset and element vectors.
 * <p>
 * An array has a one-to-many relationship with its children. Starting an array
 * prepares for writing the first element. Each element must be saved by calling
 * <tt>endValue()</tt>. This is done automatically for scalars (since there is
 * exactly one value per element), but must be done via the client code for
 * arrays of arrays or tuples. Valid state transitions:
 *
 * <table border=1>
 * <tr><th>Public API</th><th>Array Event</th><th>Offset Event</th><th>Element Event</th></tr>
 * <tr><td>startBatch()</td>
 *     <td>startWrite()</td>
 *     <td>startWrite()</td>
 *     <td>startWrite()</td></tr>
 * <tr><td>start() (new row)</td>
 *     <td>startRow()</td>
 *     <td>startRow()</td>
 *     <td>startRow()</td></tr>
 * <tr><td>start() (without save)</td>
 *     <td>restartRow()</td>
 *     <td>restartRow()</td>
 *     <td>restartRow()</td></tr>
 * <tr><td>save() (array)</td>
 *     <td>saveValue()</td>
 *     <td>saveValue()</td>
 *     <td>saveValue()</td></tr>
 * <tr><td>save() (row)</td>
 *     <td colspan=3>See subclasses.</td></tr>
 * <tr><td>harvest()</td>
 *     <td>endWrite()</td>
 *     <td>endWrite()</td>
 *     <td>endWrite()</td></tr>
 * </table>
 *
 * Some items to note:
 * <ul>
 * <li>Batch and row events are passed to the element.</li>
 * <li>Each element is saved via a call to {@link #save()} on the array.
 *     Without this call, the element value is discarded. This is necessary
 *     because the array always has an active element: no "startElement"
 *     method is necessary. This also means that any unsaved element values
 *     can be discarded simply by omitting a call to <tt>save()</tt>.</li>
 * <li>Since elements must be saved individually, the call to
 *     {@link #saveRow()} <i>does not</i> call <tt>saveValue()</tt>. This
 *     is an important distinction between an array and a tuple.</li>
 * <li>The offset and element writers are treated equally: the same events
 *     are passed to both.</li>
 * </ul>
 */

public abstract class AbstractArrayWriter implements ArrayWriter, WriterEvents {

  /**
   * Object representation of an array writer.
   */

  public static class ArrayObjectWriter extends AbstractObjectWriter {

    private AbstractArrayWriter arrayWriter;

    public ArrayObjectWriter(AbstractArrayWriter arrayWriter) {
      this.arrayWriter = arrayWriter;
    }

    @Override
    public ObjectType type() { return ObjectType.ARRAY; }

    @Override
    public void set(Object value) {
      arrayWriter.setObject(value);
    }

    @Override
    public ArrayWriter array() { return arrayWriter; }

    @Override
    protected WriterEvents baseEvents() { return arrayWriter; }

    @Override
    public void bindListener(ColumnWriterListener listener) {
      arrayWriter.bindListener(listener);
    }

    @Override
    public void bindListener(TupleWriterListener listener) {
      arrayWriter.bindListener(listener);
    }

    @Override
    public void dump(HierarchicalFormatter format) {
      format
        .startObject(this)
        .attribute("arrayWriter");
      arrayWriter.dump(format);
      format.endObject();
    }
  }

  /**
   * Index into the vector of elements for a repeated vector.
   * Keeps track of the current offset in terms of value positions.
   * Forwards overflow events to the base index.
   */

  public static class ArrayElementWriterIndex implements ColumnWriterIndex {

    private final ColumnWriterIndex baseIndex;
    private int startOffset = 0;
    private int offset = 0;

    public ArrayElementWriterIndex(ColumnWriterIndex baseIndex) {
      this.baseIndex = baseIndex;
    }

    public ColumnWriterIndex baseIndex() { return baseIndex; }

    public void reset() {
      offset = 0;
      startOffset = 0;
    }

    public int endValue() {
      startOffset = offset;
      return offset;
    }

    @Override
    public int vectorIndex() { return offset; }

    public int arraySize() {
      return offset - startOffset;
    }

    @Override
    public void nextElement() { offset++; }

    public int startOffset() { return startOffset; }

    @Override
    public void resetTo(int newIndex) {
      startOffset = 0;
      offset = newIndex;
    }

    @Override
    public String toString() {
      return new StringBuilder()
        .append("[")
        .append(getClass().getSimpleName())
        .append(" baseIndex = ")
        .append(baseIndex.toString())
        .append(", startOffset = ")
        .append(startOffset)
        .append(", offset = ")
        .append(offset)
        .append("]")
        .toString();
    }
  }

  protected final AbstractObjectWriter elementObjWriter;
  private final OffsetVectorWriter offsetsWriter;
  private ColumnWriterIndex baseIndex;
  protected ArrayElementWriterIndex elementIndex;

  public AbstractArrayWriter(RepeatedValueVector vector, AbstractObjectWriter elementObjWriter) {
    this.elementObjWriter = elementObjWriter;
    offsetsWriter = new OffsetVectorWriter(vector.getOffsetVector());
  }

  @Override
  public void bindIndex(ColumnWriterIndex index) {
    assert elementIndex != null;
    baseIndex = index;
    offsetsWriter.bindIndex(index);
    elementObjWriter.bindIndex(elementIndex);
  }

  @Override
  public int size() {
    return elementIndex.arraySize();
  }

  @Override
  public ObjectWriter entry() {
    return elementObjWriter;
  }

  @Override
  public void startWrite() {
    elementIndex.reset();
    offsetsWriter.startWrite();
    elementObjWriter.startWrite();
  }

  @Override
  public void startRow() {

    // Starting an outer value automatically starts the first
    // element value. If no elements are written, then this
    // inner start will just be ignored.

    offsetsWriter.startRow();
    elementObjWriter.startRow();
  }

  @Override
  public void saveValue() {

    // Save of the array value itself. Saves the offset vector
    // state. Does not invoke a save on children which must be
    // saved via the call to save().

    offsetsWriter.setOffset(elementIndex.endValue());
    offsetsWriter.saveValue();
  }

  @Override
  public void restartRow() {
    offsetsWriter.restartRow();
    elementIndex.resetTo(offsetsWriter.targetOffset());
    elementObjWriter.restartRow();
  }

  @Override
  public void saveRow() {
    offsetsWriter.saveRow();
    elementObjWriter.saveRow();
  }

  @Override
  public void endWrite() {
    offsetsWriter.endWrite();
    elementObjWriter.endWrite();
  }

  @Override
  public ObjectType entryType() {
    return elementObjWriter.type();
  }

  @Override
  public ScalarWriter scalar() {
    return elementObjWriter.scalar();
  }

  @Override
  public TupleWriter tuple() {
    return elementObjWriter.tuple();
  }

  @Override
  public ArrayWriter array() {
    return elementObjWriter.array();
  }

  @Override
  public void startWriteAt(int index) {
    assert false;
  }

  @Override
  public int lastWriteIndex() {
    return baseIndex.vectorIndex();
  }

  /**
   * When handling overflow, we need to know the position, within
   * the array column, of the first value for the current outer
   * (say, row) value.
   *
   * @return the offset into the array of the first value for
   * the outer value
   */

  public int lastArrayOffset() {
    return elementIndex.startOffset();
  }

  /**
   * Return the writer for the offset vector for this array. Primarily used
   * to handle overflow; other clients should not attempt to muck about with
   * the offset vector directly.
   *
   * @return the writer for the offset vector associated with this array
   */

  public OffsetVectorWriter offsetWriter() { return offsetsWriter; }

  public void resetElementIndex(int newIndex) {
    elementIndex.resetTo(newIndex);
  }

  public void bindListener(ColumnWriterListener listener) {
    elementObjWriter.bindListener(listener);
  }

  public void bindListener(TupleWriterListener listener) {
    elementObjWriter.bindListener(listener);
  }

  public void dump(HierarchicalFormatter format) {
    format
      .startObject(this)
      .attribute("elementIndex", elementIndex.vectorIndex())
      .attribute("offsetsWriter");
    offsetsWriter.dump(format);
    format.attribute("elementObjWriter");
    elementObjWriter.dump(format);
    format.endObject();
  }
}
