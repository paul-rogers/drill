/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.json;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.impl.OptionBuilder;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.store.easy.json.JsonLoader;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl.JsonOptions;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.rowSet.DirectRowSet;
import org.apache.drill.test.rowSet.RowSet;

public class JsonLoaderTestUtils {

  static class JsonTester {
    private final BufferAllocator allocator;
    public OptionBuilder loaderOptions = new OptionBuilder();
    private final JsonOptions options;
    private ResultSetLoader tableLoader;

    public JsonTester(BufferAllocator allocator, JsonOptions options) {
      this.allocator = allocator;
      this.options = options;
      options.useArrayTypes = true;
    }

    public JsonTester(BufferAllocator allocator) {
      this(allocator, new JsonOptions());
    }

    public RowSet parseFile(String resourcePath) {
      try {
        return parse(ClusterFixture.getResource(resourcePath));
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }

    public RowSet parse(String json) {
      tableLoader = new ResultSetLoaderImpl(allocator,
          loaderOptions.build());
      InputStream inStream = new
          ReaderInputStream(new StringReader(json));
      options.context = "test Json";
      JsonLoader loader = new JsonLoaderImpl(inStream, tableLoader.writer(), options);
      tableLoader.startBatch();
      while (loader.next()) {
        // No op
      }
      loader.endBatch();
      try {
        inStream.close();
      } catch (IOException e) {
        fail();
      }
      loader.close();
      return DirectRowSet.fromContainer(tableLoader.harvest());
    }

    public void close() {
      tableLoader.close();
    }
  }

  static void expectError(JsonTester tester, String json) {
      try {
        tester.parse(json);
        fail();
      } catch (UserException e) {
  //      System.out.println(e.getMessage());
        // expected
        assertTrue(e.getMessage().contains("test Json"));
      }
      tester.close();
    }

}
