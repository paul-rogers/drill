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
package org.apache.drill.exec.store.json.parser;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.List;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.project.projSet.ProjectionSetFactory;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.physical.resultSet.impl.OptionBuilder;
import org.apache.drill.exec.physical.resultSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.physical.resultSet.project.RequestedTuple;
import org.apache.drill.exec.physical.resultSet.project.RequestedTupleImpl;
import org.apache.drill.exec.physical.rowSet.DirectRowSet;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.store.easy.json.JsonLoader;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl;
import org.apache.drill.exec.store.easy.json.parser.JsonOptions;
import org.apache.drill.exec.store.easy.json.parser.TupleProjectionImpl;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.SubOperatorTest;

public abstract class BaseTestJsonLoader extends SubOperatorTest {

  static class JsonFixture {
    public final OptionBuilder loaderOptions = new OptionBuilder();
    public final JsonOptions options = new JsonOptions();
    private InputStream inStream;
    private ResultSetLoader tableLoader;
    public JsonLoader loader;
    private boolean eof;

    public JsonFixture() {
      options.context = "test Json";
    }

    public void project(List<SchemaPath> proj) {
      RequestedTuple reqTuple = RequestedTupleImpl.parse(proj);
      loaderOptions.setProjection(ProjectionSetFactory.wrap(reqTuple));
      options.rootProjection = TupleProjectionImpl.projectionFor(reqTuple);
    }

    public void openResource(String resourcePath) {
      try {
        open(ClusterFixture.getResource(resourcePath));
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }

    public void open(String json) {
      tableLoader = new ResultSetLoaderImpl(fixture.allocator(),
          loaderOptions.build());
      inStream = new
          ReaderInputStream(new StringReader(json));
      loader = new JsonLoaderImpl(inStream, tableLoader.writer(), options);
    }

    public RowSet read() {
      return read(Integer.MAX_VALUE);
    }

    public RowSet read(int maxRows) {
      if (eof) {
        return null;
      }
      tableLoader.startBatch();
      RowSetLoader writer = tableLoader.writer();
      int count = 0;
      while (writer.start()) {
        if (count == maxRows) {
          break;
        }
        eof = !loader.next();
        if (eof) {
          break;
        }
        writer.save();
        count++;
      }
      loader.endBatch();
      return DirectRowSet.fromContainer(tableLoader.harvest());
    }

    public void close() {
      // TODO: Should occur in loader
      try {
        inStream.close();
      } catch (IOException e) {
        fail();
      }
      if (loader != null) {
        loader.close();
      }
      if (tableLoader != null) {
        tableLoader.close();
      }
    }
  }

  protected static void expectError(String json) {
    JsonFixture tester = new JsonFixture();
    tester.open(json);
    expectError(tester);
    tester.close();
  }

  protected static void expectError(JsonFixture tester) {
    try {
      tester.read();
      fail();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains("test Json"));
    }
  }
}
