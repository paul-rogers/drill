package org.apache.drill.exec.ops.services.impl;

import java.io.IOException;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ops.services.FileSystemService;
import org.apache.drill.exec.ops.services.OperatorStatsService;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;

public class FileSystemServiceImpl implements FileSystemService {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemServiceImpl.class);


  private OperatorStatsService statsService;
  private DrillFileSystem fs;

  public FileSystemServiceImpl(OperatorStatsService statsService) {
    this.statsService = statsService;
  }

  @Override
  public DrillFileSystem newFileSystem(Configuration conf) throws IOException {
    Preconditions.checkState(fs == null, "Tried to create a second FileSystem. Can only be called once per OperatorContext");
    fs = new DrillFileSystem(conf, statsService);
    return fs;
  }

  /**
   * Creates a DrillFileSystem that does not automatically track operator stats.
   */
  @Override
  public DrillFileSystem newNonTrackingFileSystem(Configuration conf) throws IOException {
    Preconditions.checkState(fs == null, "Tried to create a second FileSystem. Can only be called once per OperatorContext");
    fs = new DrillFileSystem(conf, null);
    return fs;
  }

  @Override
  public void close() {
    if (fs == null) {
      return;
    }
    try {
      fs.close();
    } catch (IOException e) {
      throw UserException.resourceError(e)
        .addContext("Failure closing Drill file system")
        .build(logger);
    } finally {
      fs = null;
    }
  }
}
