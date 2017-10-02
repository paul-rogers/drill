package org.apache.drill.exec.ops.services;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.server.options.OptionsService;
import org.apache.drill.exec.testing.ExecutionControls;

public interface CoreExecService {

  /**
   * Determine if fragment execution has been interrupted.
   * @return true if execution should continue, false if an interruption has
   * occurred and fragment execution should halt
   */

  boolean shouldContinue();

  /**
   * Returns a read-only version of the session options.
   * @return the session options
   */
  OptionsService getOptionSet();

  /**
   * Returns the Drill configuration for this run. Note that the config is
   * global and immutable.
   *
   * @return the Drill configuration
   */

  DrillConfig getConfig();

  /**
   * Return the set of execution controls used to inject faults into running
   * code for testing.
   *
   * @return the execution controls
   */
  ExecutionControls getExecutionControls();
}
