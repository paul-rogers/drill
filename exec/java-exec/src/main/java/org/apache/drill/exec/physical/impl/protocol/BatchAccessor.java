package org.apache.drill.exec.physical.impl.protocol;

import java.util.Iterator;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

/**
 * Provides access to the row set (record batch) produced by an
 * operator. Previously, a record batch <i>was</i> an operator.
 * In this version, the row set is a service of the operator rather
 * than being part of the operator.
 */

public interface BatchAccessor {
  BatchSchema getSchema();
  int schemaVersion();
  int getRowCount();
  VectorContainer getOutgoingContainer();
  TypedFieldId getValueVectorId(SchemaPath path);
  VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids);
  WritableBatch getWritableBatch();
  SelectionVector2 getSelectionVector2();
  SelectionVector4 getSelectionVector4();
  Iterator<VectorWrapper<?>> iterator();
  void release();
}