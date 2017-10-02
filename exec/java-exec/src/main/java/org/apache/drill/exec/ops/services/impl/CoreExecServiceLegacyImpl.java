package org.apache.drill.exec.ops.services.impl;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.services.CoreExecService;
import org.apache.drill.exec.server.options.OptionsService;
import org.apache.drill.exec.testing.ExecutionControls;

/**
 * Wrapper for the legacy {@link FragmentContext} that exposes methods on
 * that class as an implementation of the core execution service.
 */

public class CoreExecServiceLegacyImpl implements CoreExecService {

  private final FragmentContext fragmentContext;

  public CoreExecServiceLegacyImpl(FragmentContext fragmentContext) {
    this.fragmentContext = fragmentContext;
  }

  @Override
  public boolean shouldContinue() {
    return fragmentContext.shouldContinue();
  }

  @Override
  public OptionsService getOptionSet() {
    return fragmentContext.getOptionSet();
  }

  @Override
  public DrillConfig getConfig() {
    return fragmentContext.getConfig();
  }

  @Override
  public ExecutionControls getExecutionControls() {
    return fragmentContext.getExecutionControls();
  }
}
