package org.apache.drill.exec.ops.services.impl;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.BufferManager;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.ops.services.AllocatorService;
import org.apache.drill.exec.ops.services.ExecutionControlsService;
import org.apache.drill.exec.ops.services.FragmentServices;
import org.apache.drill.exec.ops.services.OperatorScopeService;
import org.apache.drill.exec.ops.services.OperatorServices;
import org.apache.drill.exec.ops.services.OperatorStatsService;
import org.apache.drill.exec.physical.base.PhysicalOperator;

public class OperatorServicesLegacyImpl implements OperatorServices, OperatorScopeService {

  public static class AllocatorServicesShim extends AllocatorServiceImpl {
    public AllocatorServicesShim(BufferAllocator allocator, BufferManager bufferManager) {
      super(allocator, bufferManager);
    }

    @Override
    public void close() throws Exception { }
  }

  private final FragmentServices fragmentServices;
  private final OperatorContext opContext;
  private final ExecutionControlsService controlsService;
  private final AllocatorService allocatorService;

  public OperatorServicesLegacyImpl(FragmentServices fragmentServices, OperatorContext opContext) {
    this.fragmentServices = fragmentServices;
    this.opContext = opContext;
    controlsService = new ExecutionControlsServiceImpl(fragmentServices.core().getExecutionControls());
    allocatorService = new AllocatorServicesShim(opContext.getAllocator(), fragmentServices.bufferManager());
  }

  @Override
  public FragmentServices fragment() {
    return fragmentServices;
  }

  @Override
  public ExecutionControlsService controls() {
    return controlsService;
  }

  @Override
  public OperatorScopeService operator() {
    return this;
  }

  @Override
  public <T extends PhysicalOperator> T getOperatorDefn() {
    return opContext.getPopConfig();
  }

  @Override
  public BufferAllocator getAllocator() {
    return opContext.getAllocator();
  }

  @Override
  public OperatorStatsService getStats() {
    return opContext.getStats();
  }

  @Override
  public void close() {
    opContext.close();
  }

  @Override
  public AllocatorService allocator() {
    return allocatorService;
  }
}
