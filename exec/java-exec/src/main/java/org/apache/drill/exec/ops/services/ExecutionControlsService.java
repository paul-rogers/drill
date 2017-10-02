package org.apache.drill.exec.ops.services;

import org.apache.drill.exec.testing.ControlsInjector;
import org.apache.drill.exec.testing.ExecutionControls;

public interface ExecutionControlsService {

  ExecutionControls getExecutionControls();

  void setInjector(ControlsInjector injector);

  /**
   * Returns the fault injection mechanism used to introduce faults at runtime
   * for testing.
   * @return the fault injector
   */

  ControlsInjector getInjector();

  /**
   * Insert an unchecked fault (exception). Handles the details of checking if
   * fault injection is enabled and this particular fault is selected.
   * @param desc the description of the fault used to match a fault
   * injection parameter to determine if the fault should be injected
   * @throws RuntimeException an unchecked exception if the fault is enabled
   */

  void injectUnchecked(String desc);

  /**
   * Insert a checked fault (exception) of the given class. Handles the details
   * of checking if fault injection is enabled and this particular fault is
   * selected.
   *
   * @param desc the description of the fault used to match a fault
   * injection parameter to determine if the fault should be injected
   * @param exceptionClass the class of exception to be thrown
   * @throws T if the fault is enabled
   */

  <T extends Throwable> void injectChecked(String desc, Class<T> exceptionClass)
      throws T;
}
