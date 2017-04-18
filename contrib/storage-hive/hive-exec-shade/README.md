Create a shaded version of the hive-exec jar.

The hive-exec jar is an "uber" jar -- a jar with dependencies. It includes not just
the hive code, but also Hive's dependencies.

Hive includes many of the same dependencies as Drill, but different versions.
When Drill depends on hive-exec directly, the Hive dependencies get pulled into
Drill, causing conflicts.

This project reconstructs the hive-exec jar, renaming the conflictign dependencies
to a new namespace so that Drill will contain a (renamed) old copy for Hive, and
the (original named), dependent copy for Drill.

For this to work, no project that depends on Hive should ever directly depend on
hive-exec; instead it should depend on this project and exclude hive-exec:

    <dependency>
      <groupId>org.apache.drill.contrib.storage-hive</groupId>
      <artifactId>drill-hive-exec-shaded</artifactId>
      <version>${project.version}</version>
      <exclusions>
        <exclusion>
          <groupId>org.apache.hive</groupId>
          <artifactId>hive-exec</artifactId>
        </exclusion>
      </exclusions>
    </dependency>
