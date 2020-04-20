package org.apache.drill.exec.store.easy.json.loader.mongo;

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl;
import org.apache.drill.exec.store.easy.json.loader.ScalarListener;
import org.apache.drill.exec.vector.accessor.ScalarWriter;

public class StrictValueListener extends ScalarListener {

  public StrictValueListener(JsonLoaderImpl loader, ScalarWriter writer) {
    super(loader, writer);
    // TODO Auto-generated constructor stub
  }

  @Override
  protected void setArrayNull() {
    // TODO Auto-generated method stub

  }

}
