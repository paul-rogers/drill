package org.apache.drill.exec.vector.accessor.writer;

import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;

public interface OffsetVectorWriter extends ScalarWriter, WriterEvents {

  int rowStartOffset();

  int nextOffset();

  void setNextOffset(int vectorIndex);

  void dump(HierarchicalFormatter format);

}
