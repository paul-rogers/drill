package org.apache.drill.exec.store.easy.json.structparser;

import org.apache.drill.exec.store.easy.json.structparser.ObjectParser.NullArrayParser;

import com.fasterxml.jackson.core.JsonToken;

public interface ContainerListener {

  JsonElementParser forceNullArrayResolution(NullArrayParser nullArrayParser);

  JsonElementParser inferNullMember(String key);

  interface ObjectListener extends ContainerListener {

    boolean isProjected(String key);
  }

  interface ArrayListener extends ContainerListener {
  }

  JsonElementParser scalarArrayParser(String key, JsonToken token);

  ObjectListener objectListener(String key);

  JsonElementParser scalarParser(String key, JsonToken token);

  ArrayListener arrayListener(String key);

  JsonElementParser arrayParser(String key);
}