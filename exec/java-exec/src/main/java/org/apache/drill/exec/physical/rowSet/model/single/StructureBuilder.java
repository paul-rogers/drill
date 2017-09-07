package org.apache.drill.exec.physical.rowSet.model.single;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.ColumnMetadata;

public interface StructureBuilder {
  BufferAllocator allocator();
  AbstractSingleColumnModel addColumn(AbstractSingleTupleModel tuple, ColumnMetadata schema);
}