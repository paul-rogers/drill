package org.apache.drill.exec.store.http;

import java.io.File;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.store.easy.json.loader.JsonLoader;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderOptions;
import org.apache.drill.exec.store.http.util.SimpleHttp;

public class HttpBatchReader implements ManagedReader<SchemaNegotiator> {
  private final HttpStoragePluginConfig config;
  private final HttpSubScan subScan;
  private SimpleHttp http;
  private ResultSetLoader loader;
  private JsonLoader jsonLoader;

  public HttpBatchReader(HttpStoragePluginConfig config, HttpSubScan subScan) {
    this.config = config;
    this.subScan = subScan;
  }

  @Override
  public boolean open(SchemaNegotiator negotiator) {
    String tempDirPath = negotiator
        .drillConfig()
        .getString(ExecConstants.DRILL_TMP_DIR);
    File tmpDir = new File(tempDirPath);
    String connectionName = subScan.tableSpec().database();
    this.http = new SimpleHttp(config, tmpDir, connectionName);
    String url = subScan.getFullURL();
    return http.getInputStream(url);
    loader = negotiator.build();
    JsonLoaderOptions jsonOptions = new JsonLoaderOptions(negotiator.queryOptions());
    jsonLoader = new JsonLoaderBuilder()
        .resultSetLoader(loader)
        .options(jsonOptions)
        .errorContext(errorContext)
        .fromReader(queryResponse.reader())
        .messageParser(this)
        .build();
    return true;
  }

  @Override
  public boolean next() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

}
