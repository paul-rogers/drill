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
import java.util.List;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.scan.file.BaseFileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.ResultVectorCache;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.easy.json.JsonLoaderImpl.JsonOptions;
import org.apache.drill.exec.store.easy.json.JsonLoaderImpl.TypeNegotiator;
import org.apache.hadoop.mapred.FileSplit;

public class JsonBatchReader implements ManagedReader<FileSchemaNegotiator> {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JsonBatchReader.class);

  private final DrillFileSystem fileSystem;
  private final FileSplit split;
  private final JsonOptions options;
  private JsonLoader jsonLoader;
  private InputStream stream;

  private RowSetLoader tableLoader;

  public JsonBatchReader(FileSplit split, DrillFileSystem dfs, JsonOptions options) {
    this.fileSystem = dfs;
    this.split = split;
    this.options = options == null ? new JsonOptions() : options;
  }

  @Override
  public boolean open(FileSchemaNegotiator negotiator) {
    OperatorContext opContext = negotiator.context();
    OptionSet optionMgr = opContext.getFragmentContext().getOptions();
    Object embeddedContent = null;
    options.allTextMode = embeddedContent == null && optionMgr.getBoolean(ExecConstants.JSON_ALL_TEXT_MODE);
    options.readNumbersAsDouble = embeddedContent == null && optionMgr.getBoolean(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE);
    options.unionEnabled = embeddedContent == null && optionMgr.getBoolean(ExecConstants.ENABLE_UNION_TYPE_KEY);
    options.skipMalformedRecords = optionMgr.getBoolean(ExecConstants.JSON_READER_SKIP_INVALID_RECORDS_FLAG);
    // Printing of malformed records is always enabled.
//    options.printSkippedMalformedJSONRecordLineNumber = optionMgr.getBoolean(ExecConstants.JSON_READER_PRINT_INVALID_RECORDS_LINE_NOS_FLAG);
    options.allowNanInf = true;

    try {
      stream = fileSystem.openPossiblyCompressedStream(split.getPath());
    } catch (IOException e) {
      throw UserException
          .dataReadError(e)
          .addContext("Failure to open JSON file", split.getPath().toString())
          .build(logger);
    }
    final ResultSetLoader rsLoader = negotiator.build();
    tableLoader = rsLoader.writer();
    RowSetLoader rootWriter = tableLoader;

    // Bind the type negotiator that will resolve ambiguous types
    // using information from any previous readers in this scan.

    options.typeNegotiator = new TypeNegotiator() {
      @Override
      public MajorType typeOf(List<String> path) {
        ResultVectorCache cache = rsLoader.vectorCache();
        for (int i = 0; i < path.size() - 1; i++) {
          cache = cache.childCache(path.get(i));
        }
        return cache.getType(path.get(path.size()-1));
      }
    };

    // Create the JSON loader (high-level parser).

    jsonLoader = new JsonLoaderImpl(stream, rootWriter, options);
    return true;
  }

  @Override
  public boolean next() {
    boolean more = true;
    while (! tableLoader.isFull()) {
      if (! jsonLoader.next()) {
        more = false;
        break;
      }
    }
    jsonLoader.endBatch();
    return more;
  }

  @Override
  public void close() {
    if (stream != null) {
      try {
        stream.close();
      } catch (Exception e) {

        // Ignore errors

        logger.warn("Ignored failure closing JSON file: " + split.getPath().toString(), e);
      } finally {
        stream = null;
      }
    }
    if (jsonLoader != null) {
      try {
        jsonLoader.close();
      } catch (Exception e) {

        // Ignore errors

        logger.warn("Ignored failure closing JSON loader for file: " + split.getPath().toString(), e);
      } finally {
        jsonLoader = null;
      }
    }
  }
}
