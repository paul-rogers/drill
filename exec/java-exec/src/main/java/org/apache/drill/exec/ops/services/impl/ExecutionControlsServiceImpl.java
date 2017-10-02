package org.apache.drill.exec.ops.services.impl;

import org.apache.drill.exec.ops.services.ExecutionControlsService;
import org.apache.drill.exec.testing.ControlsInjector;
import org.apache.drill.exec.testing.ExecutionControls;

public class ExecutionControlsServiceImpl implements ExecutionControlsService {

  private final ExecutionControls controls;
  private ControlsInjector injector;

  public ExecutionControlsServiceImpl(ExecutionControls controls) {
    this.controls = controls;
  }

  @Override
  public void setInjector(ControlsInjector injector) {
    this.injector = injector;
  }

  @Override
  public ExecutionControls getExecutionControls() {
    return controls;
  }

  @Override
  public ControlsInjector getInjector() {
    return injector;
  }

  @Override
  public void injectUnchecked(String desc) {
    if (injector != null  &&  controls != null) {
      injector.injectUnchecked(controls, desc);
    }
  }

  @Override
  public <T extends Throwable> void injectChecked(String desc, Class<T> exceptionClass)
      throws T {
    if (injector != null  &&  controls != null) {
      injector.injectChecked(controls, desc, exceptionClass);
    }
  }
}
