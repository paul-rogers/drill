package org.apache.drill.exec.ops.services.impl;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.BufferManager;
import org.apache.drill.exec.ops.services.AllocatorService;

import io.netty.buffer.DrillBuf;

public class AllocatorServiceImpl implements AllocatorService {

  private final BufferAllocator allocator;
  private final BufferManager bufferManager;

  public AllocatorServiceImpl(BufferAllocator allocator, BufferManager bufferManager) {
    this.allocator = allocator;
    this.bufferManager = bufferManager;
  }

  @Override
  public DrillBuf replace(DrillBuf old, int newSize) {
    return bufferManager.replace(old, newSize);
  }

  @Override
  public DrillBuf getManagedBuffer() {
    return bufferManager.getManagedBuffer();
  }

  @Override
  public DrillBuf getManagedBuffer(int size) {
    return bufferManager.getManagedBuffer(size);
  }

  @Override
  public BufferAllocator allocator() {
    return allocator;
  }

  @Override
  public void close() throws Exception {
    allocator.close();
  }
}
