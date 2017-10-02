package org.apache.drill.exec.ops.services;

import org.apache.drill.exec.ops.BufferManager;

/**
 * Successor to {@link FragmentContext}: offers a series of
 * services rather than a tightly-coupled collection of methods.
 * Allows easily mocking the services for testing.
 */
public interface FragmentServices {
  CoreExecService core();
  CodeGenService codeGen();
  BufferManager bufferManager();
}
