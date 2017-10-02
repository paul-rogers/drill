package org.apache.drill.exec.ops.services;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.base.PhysicalOperator;

/**
 * Provides operator-specific context such as the operator
 * definition, the operator's allocator, the operators's
 * stats, etc.
 */

public interface OperatorScopeService {

  /**
   * Return the physical operator definition created by the planner and passed
   * into the Drillbit executing the query.
   * @return the physical operator definition
   */

  <T extends PhysicalOperator> T getOperatorDefn();

  /**
   * Return the memory allocator for this operator.
   *
   * @return the per-operator memory allocator
   */

  BufferAllocator getAllocator();

  /**
   * A write-only interface to the Drill statistics mechanism. Allows
   * operators to update statistics.
   * @return operator statistics
   */

  OperatorStatsService getStats();
}
