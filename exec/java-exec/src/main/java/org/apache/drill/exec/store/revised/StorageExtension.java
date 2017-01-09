package org.apache.drill.exec.store.revised;

import org.apache.drill.exec.store.revised.Sketch.LogicalSchema;

/**
 * Defines a singleton, persistent representation of a table space which resides
 * each Drillbit. Drill plugin instances provide a window to this singleton.
 */

public interface StorageExtension {
  LogicalSchema rootSchema();
}
