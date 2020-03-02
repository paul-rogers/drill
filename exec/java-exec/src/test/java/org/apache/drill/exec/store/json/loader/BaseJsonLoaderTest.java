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
package org.apache.drill.exec.store.json.loader;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.EmptyErrorContext;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.impl.OptionBuilder;
import org.apache.drill.exec.physical.resultSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.easy.json.loader.JsonLoader;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderOptions;
import org.apache.drill.test.SubOperatorTest;

public class BaseJsonLoaderTest extends SubOperatorTest {

  protected static class JsonLoaderFixture {

    public OptionBuilder rsLoaderOptions = new OptionBuilder();
    public TupleMetadata providedSchema;
    public JsonLoaderOptions jsonOptions = new JsonLoaderOptions();
    public CustomErrorContext errorContext = new EmptyErrorContext();
    private ResultSetLoader rsLoader;
    private JsonLoader loader;

    public void open(InputStream is) {
      rsLoader = new ResultSetLoaderImpl(fixture.allocator(), rsLoaderOptions.build());
      loader = new JsonLoaderImpl(rsLoader, providedSchema, jsonOptions, errorContext, is);
    }

    public void open(String json) {
      InputStream stream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
      open(stream);
    }

    public RowSet next() {
      if (!loader.next()) {
        return null;
      }
      return fixture.wrap(rsLoader.harvest());
    }

    public void close() {
      loader.close();
      rsLoader.close();
    }
  }
}
