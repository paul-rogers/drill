package org.apache.drill.exec.compile;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.MapMaker;

/**
 * Class loader for "plain-old Java" generated classes.
 * Very basic implementation: allows defining a class from
 * byte codes and finding the loaded classes. Delegates
 * all other class requests to the thread context class
 * loader. This structure ensures that a generated class can
 * find both its own inner classes as well as all the standard
 * Drill implementation classes.
 */

public class CachedClassLoader extends URLClassLoader {

  /**
   * Cache of generated classes. Semantics: a single thread defines
   * the classes, many threads may access the classes.
   */

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
  public Class<?> findClass(String className) throws ClassNotFoundException {
    Class<?> theClass = cache.get( className );
    if ( theClass != null )
      return theClass;
    return super.findClass(className);
  }

  public void addClasses(Map<String, byte[]> results) {
    for ( String key : results.keySet() ) {
      addClass( key, results.get( key ) );
    }
  }
}
