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
package org.apache.drill.exec.store.easy.json;

import java.io.IOException;
import java.io.InputStream;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.impl.scan.file.BaseFileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.hadoop.mapred.FileSplit;

public class JsonBatchReader implements ManagedReader<FileSchemaNegotiator> {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JsonBatchReader.class);

  private final DrillFileSystem fileSystem;
  private final FileSplit split;
  private JsonLoader jsonLoader;
  private InputStream stream;

  private RowSetLoader tableLoader;

  public JsonBatchReader(DrillFileSystem dfs, FileSplit split) {
    this.fileSystem = dfs;
    this.split = split;
  }

  @Override
  public boolean open(FileSchemaNegotiator negotiator) {
    try {
      stream = fileSystem.openPossiblyCompressedStream(split.getPath());
    } catch (IOException e) {
      throw UserException
          .dataReadError(e)
          .addContext("Failure to open JSON file", split.getPath().toString())
          .build(logger);
    }
    tableLoader = negotiator.build().writer();
    TupleWriter rootWriter = tableLoader;

    // TODO: Pass in options

    jsonLoader = new JsonLoaderImpl(stream, rootWriter);
    return true;
  }

  @Override
  public boolean next() {
    if (! jsonLoader.next()) {
      return false;
    }
    while (! tableLoader.isFull()) {
      if (! jsonLoader.next()) {
        break;
      }
    }
    return true;
  }

  @Override
  public void close() {
    if (stream != null) {
      try {
        stream.close();
      } catch (Exception e) {

        // Ignore errors

        logger.warn("Failure closing JSON file: " + split.getPath().toString(), e);
      } finally {
        stream = null;
      }
    }
    if (jsonLoader != null) {
      try {
        jsonLoader.close();
      } catch (Exception e) {

        // Ignore errors

        logger.warn("Failure closing JSON loader for file: " + split.getPath().toString(), e);
      } finally {
        jsonLoader = null;
      }
    }
  }

}
