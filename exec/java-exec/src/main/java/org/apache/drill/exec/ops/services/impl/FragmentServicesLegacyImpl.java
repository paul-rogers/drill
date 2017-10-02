package org.apache.drill.exec.ops.services.impl;

import org.apache.drill.exec.ops.BufferManager;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.services.CodeGenService;
import org.apache.drill.exec.ops.services.CoreExecService;
import org.apache.drill.exec.ops.services.FragmentServices;

/**
 * Wrapper around the legacy {@link FragmentContext} (tightly coupled
 * implementation of many service) to expose it as the new, loosely
 * coupled collection of services.
 */

public class FragmentServicesLegacyImpl implements FragmentServices {

  private final FragmentContext fragmentContext;

  public FragmentServicesLegacyImpl(FragmentContext fragmentContext) {
    this.fragmentContext = fragmentContext;
  }

  @Override
  public CoreExecService core() {
    return fragmentContext;
  }

  @Override
  public CodeGenService codeGen() {
    return fragmentContext;
  }

  @Override
  public BufferManager bufferManager() {
    return fragmentContext;
  }

}
