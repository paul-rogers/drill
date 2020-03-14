package org.apache.drill.exec.physical.impl.scan.v3.file;

import org.apache.drill.common.exceptions.ChildErrorContext;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException.Builder;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.v3.ReaderFactory;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader.EarlyEofException;
import org.apache.drill.exec.physical.impl.scan.v3.lifecycle.ReaderLifecycle;
import org.apache.drill.exec.physical.impl.scan.v3.lifecycle.SchemaNegotiatorImpl;
import org.apache.drill.exec.physical.impl.scan.v3.lifecycle.StaticBatchBuilder;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

/**
 * Implementation of the file-level schema negotiator which holds the
 * file split which the reader is to process. This class presents the
 * split in both Hadoop and Drill formats. Adds the file name to the
 * error context.
 */
public class FileSchemaNegotiatorImpl extends SchemaNegotiatorImpl
    implements FileSchemaNegotiator {

  public class SplitErrorContext extends ChildErrorContext {

    public SplitErrorContext(CustomErrorContext parent) {
      super(parent);
    }

    @Override
    public void addContext(Builder builder) {
      super.addContext(builder);
      builder.addContext("File", Path.getPathWithoutSchemeAndAuthority(split.getPath()).toString());
      if (split.getStart() != 0) {
        builder.addContext("Offset", split.getStart());
        builder.addContext("Length", split.getLength());
      }
    }
  }

  private FileWork fileWork;
  private FileSplit split;

  public FileSchemaNegotiatorImpl(ReaderLifecycle readerLifecycle) {
    super(readerLifecycle);
    baseErrorContext = new SplitErrorContext(baseErrorContext);
    readerErrorContext = baseErrorContext;
  }

  public void bindSplit(FileWork fileWork) {
    this.fileWork = fileWork;
    Path path = fileScan().fileSystem().makeQualified(fileWork.getPath());
    split = new FileSplit(path, fileWork.getStart(), fileWork.getLength(), new String[]{""});
  }

  @Override
  public DrillFileSystem fileSystem() { return fileScan().fileSystem(); }

  @Override
  public FileSplit split() { return split; }

  @Override
  public FileWork fileWork() { return fileWork; }

  @Override
  @SuppressWarnings("unchecked")
  public ManagedReader newReader(ReaderFactory<?> readerFactory) throws EarlyEofException {
    return ((ReaderFactory<FileSchemaNegotiator>) readerFactory).next(this);
  }

  @Override
  public StaticBatchBuilder implicitColumnsLoader() {
    return fileScan().implicitColumnsHandler().forFile(split.getPath());
  }

  private FileScanLifecycle fileScan() {
    return (FileScanLifecycle) readerLifecycle.scanLifecycle();
  }
}
