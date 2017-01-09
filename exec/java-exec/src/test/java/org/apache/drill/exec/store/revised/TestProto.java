package org.apache.drill.exec.store.revised;

import static org.junit.Assert.*;

import org.apache.drill.exec.store.revised.proto.ProtoPluginConfig;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.FixtureBuilder;
import org.junit.Test;

public class TestProto {

  @Test
  public void test() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        ;
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.definePluginConfig("proto", new ProtoPluginConfig());
      client.queryBuilder().sql("SELECT * FROM `proto`.`atable`").printCsv();
    }
  }

}
