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

import java.io.File;

import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.store.easy.json.loader.JsonLoader;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl.JsonLoaderBuilder;
import org.apache.drill.exec.store.http.util.SimpleHttp;

public class HttpBatchReader implements ManagedReader<SchemaNegotiator> {
  private final HttpStoragePluginConfig config;
  private final HttpSubScan subScan;
  private CustomErrorContext errorContext;
  private SimpleHttp http;
  private ResultSetLoader loader;
  private JsonLoader jsonLoader;

  public HttpBatchReader(HttpStoragePluginConfig config, HttpSubScan subScan) {
    this.config = config;
    this.subScan = subScan;
  }

  @Override
  public boolean open(SchemaNegotiator negotiator) {
    errorContext = negotiator.parentErrorContext();
    String tempDirPath = negotiator
        .drillConfig()
        .getString(ExecConstants.DRILL_TMP_DIR);
    this.http = new SimpleHttp(config, new File(tempDirPath),
        subScan.tableSpec().database(), errorContext);
    loader = negotiator.build();
    jsonLoader = new JsonLoaderBuilder()
        .resultSetLoader(loader)
        .standardOptions(negotiator.queryOptions())
        .errorContext(errorContext)
        .fromStream(http.getInputStream(subScan.getFullURL()))
        .build();
    return true;
  }

  @Override
  public boolean next() {
    return jsonLoader.readBatch();
  }

  @Override
  public void close() {
    if (jsonLoader != null) {
      jsonLoader.close();
      jsonLoader = null;
    }
  }
}
