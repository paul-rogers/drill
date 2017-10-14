package org.apache.drill.exec.physical.impl.scan.file;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.fs.Path;

/**
 * Specify the file name and optional selection root. If the selection root
 * is provided, then partitions are defined as the portion of the file name
 * that is not also part of the selection root. That is, if selection root is
 * /a/b and the file path is /a/b/c/d.csv, then dir0 is c.
 */

public class FileMetadata {

  private final Path filePath;
  private final String[] dirPath;

  public FileMetadata(Path filePath, Path selectionRoot) {
    this.filePath = filePath;

    // If the data source is not a file, no file metadata is available.

    if (selectionRoot == null || filePath == null) {
      dirPath = null;
      return;
    }

    // Result of splitting /x/y is ["", "x", "y"], so ignore first.

    String[] r = Path.getPathWithoutSchemeAndAuthority(selectionRoot).toString().split("/");

    // Result of splitting "/x/y/z.csv" is ["", "x", "y", "z.csv"], so ignore first and last

    String[] p = Path.getPathWithoutSchemeAndAuthority(filePath).toString().split("/");

    if (p.length - 1 < r.length) {
      throw new IllegalArgumentException("Selection root of \"" + selectionRoot +
                                      "\" is shorter than file path of \"" + filePath.toString() + "\"");
    }
    for (int i = 1; i < r.length; i++) {
      if (! r[i].equals(p[i])) {
        throw new IllegalArgumentException("Selection root of \"" + selectionRoot +
            "\" is not a leading path of \"" + filePath.toString() + "\"");
      }
    }
    dirPath = ArrayUtils.subarray(p, r.length, p.length - 1);
  }

  public Path filePath() { return filePath; }

  public String partition(int index) {
    if (dirPath == null ||  dirPath.length <= index) {
      return null;
    }
    return dirPath[index];
  }

  public int dirPathLength() {
    return dirPath == null ? 0 : dirPath.length;
  }

  public boolean isSet() { return filePath != null; }
}