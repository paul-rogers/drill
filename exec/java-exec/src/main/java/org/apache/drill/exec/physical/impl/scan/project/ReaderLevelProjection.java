package org.apache.drill.exec.physical.impl.scan.project;

import java.util.List;

import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ColumnProjection;

public interface ReaderLevelProjection {

  List<ColumnProjection> output();
}
