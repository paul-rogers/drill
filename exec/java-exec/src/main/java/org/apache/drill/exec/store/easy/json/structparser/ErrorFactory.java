package org.apache.drill.exec.store.easy.json.structparser;

import java.io.IOException;

import org.apache.drill.exec.vector.accessor.UnsupportedConversionError;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonToken;

public interface ErrorFactory {

  RuntimeException parseError(String string, JsonParseException e);

  RuntimeException ioException(IOException e);

  RuntimeException structureError(String string);

  RuntimeException syntaxError(JsonParseException e);

  RuntimeException typeError(UnsupportedConversionError e);

  RuntimeException ioException(String string, IOException e);

  RuntimeException syntaxError(JsonToken token);

  RuntimeException unrecoverableError();

  RuntimeException structureError(String key, JsonToken token);
}
