package org.apache.drill.exec.physical.impl.scan.project;

import java.util.List;

import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ColumnProjection;

public class NullReaderProjection implements ReaderLevelProjection {

  private final ScanLevelProjection scanProj;

  public NullReaderProjection(ScanLevelProjection scanProj) {
    this.scanProj = scanProj;
  }

  @Override
  public List<ColumnProjection> output() {
    return scanProj.outputCols();
  }

}
