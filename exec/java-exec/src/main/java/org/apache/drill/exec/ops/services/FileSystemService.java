package org.apache.drill.exec.ops.services;

import java.io.IOException;

import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.hadoop.conf.Configuration;

public interface FileSystemService {

  DrillFileSystem newFileSystem(Configuration conf) throws IOException;

  DrillFileSystem newNonTrackingFileSystem(Configuration conf) throws IOException;

  void close();
}
