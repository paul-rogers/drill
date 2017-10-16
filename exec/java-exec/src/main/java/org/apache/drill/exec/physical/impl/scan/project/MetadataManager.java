package org.apache.drill.exec.physical.impl.scan.project;

import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ScanProjectionParser;
import org.apache.drill.exec.physical.impl.scan.project.TableLevelProjection.TableProjectionResolver;

public interface MetadataManager {
  ScanProjectionParser projectionParser();

  ReaderLevelProjection resolve(ScanLevelProjection scanProj);

  TableProjectionResolver resolver();

  void load(int rowCount);
}
