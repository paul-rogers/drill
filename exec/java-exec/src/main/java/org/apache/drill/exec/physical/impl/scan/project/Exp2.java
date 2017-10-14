package org.apache.drill.exec.physical.impl.scan.project;

import org.apache.drill.common.types.TypeProtos.MajorType;

public class Exp2 {

  public interface ColumnProjection {

    public static final int WILDCARD = 1;
    public static final int UNRESOLVED = 2;
    public static final int NULL = 5;
    public static final int PROJECTED = 6;

    String name();
    int nodeType();
    MajorType type();
  }

  public static class WildcardColumn {

  }

  public static class UnresolvedColumn {

  }

  public static class NullColumn {

  }

  public static class ProjectedColumn {

  }

  public static class FileMetadataColumn {

  }

  public static class PartitionColumn {

  }
}
