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

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.store.easy.json.JSONRecordReader;
import org.apache.drill.exec.store.http.util.SimpleHttp;

import java.io.InputStream;
import java.util.List;


public class HttpRecordReader extends JSONRecordReader {

  private final HttpSubScan subScan;

  private final SimpleHttp http;

  public HttpRecordReader(FragmentContext context, List<SchemaPath> projectedColumns, HttpStoragePluginConfig config, HttpSubScan subScan) {
    super(context, projectedColumns);
    this.subScan = subScan;
    this.http = new SimpleHttp(config, context);
    InputStream inputStream = getInputStream();

    setInputStream(inputStream);
  }
  /**
   * Executes the HTTP request and returns an InputStream to the retrieved data
   * @return InputStream the InputStream containing the data
   */
  private InputStream getInputStream() {
    String url = subScan.getFullURL();
    return http.getInputStream(url);
  }
}
