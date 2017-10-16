package org.apache.drill.exec.physical.impl.scan.project;

import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ScanProjectionParser;

public class NullMetadataManager implements MetadataManager {

  @Override
  public ScanProjectionParser projectionParser() { return null; }

  @Override
  public ReaderLevelProjection resolve(ScanLevelProjection scanProj) {
    return new NullReaderProjection(scanProj);
  }

}
