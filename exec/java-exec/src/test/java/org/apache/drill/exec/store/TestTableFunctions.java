package org.apache.drill.exec.store;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class TestTableFunctions {

  public static class TestClass {
    private final String foo;
    private final String bar;

    @JsonCreator
    public TestClass(@JsonProperty("foo") String foo,
        @JsonProperty("bar") String bar) {
      this.foo = foo;
      this.bar = bar;
    }

    public String getFoo() { return foo; }
    public String getBar() { return bar; }
  }

  @Test
  public void testAdHoc() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectWriter writer = mapper.writerFor(TestClass.class);
    TestClass test1 = new TestClass("foo", "bar");

    JsonNode node = mapper.valueToTree(test1);
    assertTrue(node.isObject());
    ObjectNode obj = (ObjectNode) node;
    obj.put("bar", "fred");

    ObjectReader reader = mapper.readerFor(TestClass.class);
//    TestClass test3 = reader.readValue(ser1);
//    System.out.println(test3);

    TestClass test2 = reader.readValue(node);

    System.out.println(writer.writeValueAsString(test2));
  }
}
