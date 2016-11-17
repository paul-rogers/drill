package org.apache.drill.exec.codegen;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.MapMaker;

public class CachedClassLoader extends URLClassLoader {

  private ConcurrentMap<String, Class<?>> cache = new MapMaker().concurrencyLevel(4).makeMap();

  public CachedClassLoader( ) {
    super(new URL[0], Thread.currentThread().getContextClassLoader());
  }

  public void addClass( String fqcn, byte[] byteCodes ) {

    assert ! cache.containsKey( fqcn );
    Class<?> newClass = defineClass(fqcn, byteCodes, 0, byteCodes.length);
    cache.put( fqcn, newClass );
  }

  @Override
  protected Class<?> findClass(String className) throws ClassNotFoundException {
    Class<?> theClass = cache.get( className );
    if ( theClass != null )
      return theClass;
    return super.findClass(className);
  }

}
