package org.apache.drill.exec.ops.services;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.BufferManager;

public interface AllocatorService extends BufferManager {
  BufferAllocator allocator();
}
