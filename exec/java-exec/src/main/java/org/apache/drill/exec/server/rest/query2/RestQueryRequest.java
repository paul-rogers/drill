package org.apache.drill.exec.server.rest.query2;

import java.io.IOException;
import java.util.List;

import org.apache.drill.exec.query.StatementParser;

public class RestQueryRequest {
  private final List<String> cmds;
  private final int rowLimit;

  public RestQueryRequest(Builder builder) {
    this.cmds = builder.cmds;
    this.rowLimit = builder.rowLimit;
  }

  public int rowLimit() { return rowLimit; }

  public static Builder builder() { return new Builder(); }

  public static class Builder {

    private String input;
    private List<String> cmds;
    private int rowLimit;

    public Builder query(String sql) {
      input = sql;
      cmds.add(sql);
      return this;
    }

    public Builder commands(String input) {
      this.input = input;
      StatementParser parser = new StatementParser(input);
      while (true) {
        String stmt;
        try {
          stmt = parser.parseNext();
        } catch (IOException e) {
          throw new IllegalStateException("Won't occur for a string.");
        }
        if (stmt == null) {
          break;
        } else {
          cmds.add(stmt);
        }
      }
      return this;
    }

    public RestQueryRequest build() {
      return new RestQueryRequest(this);
    }

  }
}
