package org.apache.drill.exec.server.rest.query2;

import java.util.List;

import org.apache.drill.exec.proto.UserBitShared.QueryId;

public class QueryRequest {

  public enum ResponseType {
    JSON,
    JSON_LINES,
    CSV,
    MSGPACK
  }
  private List<String> queries;
  private int rowLimit;
  private ResponseType responseType;

  public static class RestQueryBuilder {

  }
  public static class QueryRunner {

  }
}
