package org.apache.drill.exec.store.easy.json.structparser;

public interface ValueListener {

  void onBoolean(boolean value);
  void onNull();
  void onInt(long value);
  void onFloat(double value);
  void onString(String value);
  void onEmbedddObject(String value);
}
