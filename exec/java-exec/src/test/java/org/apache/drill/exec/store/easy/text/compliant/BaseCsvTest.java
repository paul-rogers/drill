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
package org.apache.drill.exec.store.easy.text.compliant;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.store.easy.text.TextFormatPlugin.TextFormatConfig;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;

public class BaseCsvTest extends ClusterTest {

  protected static String validHeaders[] = {
      "a,b,c",
      "10,foo,bar"
  };

  protected static File testDir;

  protected static void setup(boolean skipFirstLine, boolean extractHeader) throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher).maxParallelization(1));

    // Set up CSV storage plugin using headers.

    TextFormatConfig csvFormat = new TextFormatConfig();
    csvFormat.fieldDelimiter = ',';
    csvFormat.skipFirstLine = skipFirstLine;
    csvFormat.extractHeader = extractHeader;

    testDir = cluster.makeDataDir("data", "csv", csvFormat);
  }

  protected void enableV3(boolean enable) {
    client.alterSession(ExecConstants.ENABLE_V3_TEXT_READER_KEY, enable);
  }

  protected void resetV3() {
    client.resetSession(ExecConstants.ENABLE_V3_TEXT_READER_KEY);
  }

  protected static void buildFile(String fileName, String[] data) throws IOException {
    buildFile(new File(testDir, fileName), data);
  }

  protected static void buildFile(File file, String[] data) throws IOException {
    try(PrintWriter out = new PrintWriter(new FileWriter(file))) {
      for (String line : data) {
        out.println(line);
      }
    }
  }

}
