package org.apache.drill.exec.ops.services;

/**
 * Skeleton implementation of an operator execution context. The concept is
 * <ul>
 * <li>One object that provides services to an operation execution.</li>
 * <li>Wraps fragment-level services to avoid need to pass in a fragment context.</li>
 * <li>Wraps common operator-level services to avoid redundant code.</li>
 * <li>Provides access to the operator definition, to avoid need to pass that
 * along everywhere needed.</li>
 * <li>Defined as an interface to allow a test-time mock without the need
 * for a full Drillbit.</li>
 * </ul>
 * The full collection of services will be large. To start, this is just a
 * wrapper around the existing contexts. Over time, we can add methods here
 * rather than referring to the other contexts. In the end, the other operator
 * contexts can be deprecated.
 */

public interface OperatorServices {
  AllocatorService allocator();
  FragmentServices fragment();
  ExecutionControlsService controls();
  OperatorScopeService operator();
  void close();
}
