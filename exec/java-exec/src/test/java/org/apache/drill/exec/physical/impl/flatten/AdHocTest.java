package org.apache.drill.exec.physical.impl.flatten;

import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class AdHocTest extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
  }

  @Test
  public void test() throws Exception {
    String sql = "select flatten(sub1.events) flat_events  from "+
        "(select t1.events events from cp.`complex/json/flatten_join.json` t1 "+
        "inner join cp.`complex/json/flatten_join.json` t2 on t1.id=t2.id) sub1";
    client.queryBuilder().sql(sql).print();
  }

}
