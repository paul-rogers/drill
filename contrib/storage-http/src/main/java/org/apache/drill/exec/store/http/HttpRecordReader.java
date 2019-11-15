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

package org.apache.drill.exec.store.http;

import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.ExecConstants;

import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.http.util.JsonConverter;
import org.apache.drill.exec.store.http.util.SimpleHttp;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.fn.JsonReader;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

public class HttpRecordReader extends AbstractRecordReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HttpRecordReader.class);

  private VectorContainerWriter writer;
  private JsonReader jsonReader;
  private FragmentContext fragmentContext;
  private HttpSubScan subScan;
  private Iterator<JsonNode> jsonIt;
  private JsonNode root;
  private ResultSetLoader loader;
  private final boolean enableAllTextMode;
  private final boolean enableNanInf;
  private final boolean readNumbersAsDouble;
  private final HttpStoragePluginConfig config;

  public HttpRecordReader(FragmentContext context, List<SchemaPath> projectedColumns, HttpStoragePluginConfig config, HttpSubScan subScan) {
    this.config = config;
    this.subScan = subScan;
    fragmentContext = context;
    setColumns(projectedColumns);

    enableAllTextMode = fragmentContext.getOptions().getOption(ExecConstants.JSON_ALL_TEXT_MODE).bool_val;
    enableNanInf = fragmentContext.getOptions().getOption(ExecConstants.JSON_READER_NAN_INF_NUMBERS).bool_val;
    readNumbersAsDouble = fragmentContext.getOptions().getOption(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE).bool_val;
  }

  @Override
  public void setup(OperatorContext context,
                    OutputMutator output)
    throws ExecutionSetupException {
    this.writer = new VectorContainerWriter(output);
    this.jsonReader = new JsonReader.Builder(fragmentContext.getManagedBuffer())
      .schemaPathColumns(Lists.newArrayList(getColumns()))
      .allTextMode(enableAllTextMode)
      .readNumbersAsDouble(readNumbersAsDouble)
      .enableNanInf(enableNanInf)
      .build();
    String q = subScan.getURL();
    if (q.startsWith("file://")) {
      loadFile();
    } else {
      loadHttp();
    }
  }

  private void loadHttp() {
    String url = subScan.getFullURL();
    SimpleHttp http = new SimpleHttp();
    String content = http.get(url);
    logger.info("http '{}' response {} bytes", url, content.length());
    parseResult(content);
  }

  private void loadFile() {
    logger.debug("load local file {}", subScan.getScanSpec().getURI());
    String file = subScan.getScanSpec().getURI().substring("file://".length() - 1);
    String content = JsonConverter.stringFromFile(file);
    parseResult(content);
  }

  private void parseResult(String content) {
    String key = subScan.getStorageConfig().getResultKey();
    this.root = key.length() == 0 ? JsonConverter.parse(content) : JsonConverter.parse(content, key);
    if (root != null) {
      logger.debug("response object count {}", root.size());
      jsonIt = root.elements();
    }
  }


  @Override
  public int next() {
    logger.info("HttpRecordReader next");
    if (jsonIt == null || !jsonIt.hasNext()) {
      return 0;
    }
    writer.allocate();
    writer.reset();
    int docCount = 0;
    try {
      //while (docCount < BaseValueVector.INITIAL_VALUE_ALLOCATION && jsonIt.hasNext()) {
        writer.rootAsMap();
        jsonReader.setSource(root);
        writer.setPosition(docCount);
        jsonReader.write(writer);
        root = jsonIt.next();
        docCount++;
      //}

      jsonReader.ensureAtLeastOneField(writer);
    } catch (Exception e) {

    }
    writer.setValueCount(docCount);
    return docCount;
  }

  @Override
  public void close() {
    logger.debug("HttpRecordReader cleanup");
    try {
      writer.close();
    } catch (Exception e) {
      // Do something...
    }
  }
}
