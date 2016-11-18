package org.apache.drill.exec.codegen;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.drill.exec.compile.ClassCompilerSelector;
import org.apache.drill.exec.compile.ClassTransformer;
import org.apache.drill.exec.compile.ClassTransformer.ClassNames;
import org.apache.drill.exec.compile.JDKClassCompiler;
import org.apache.drill.exec.compile.QueryClassLoader;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.codehaus.commons.compiler.CompileException;

public abstract class CodeBuilder<T> {

  FragmentContext context;
  CodeGenerator<T> cg;
  private CachedClassLoader classLoader;
  boolean straightJava;
  boolean useCache = true;
  boolean saveCode = true;
  File codeDir = new File( "/tmp/code" );
  boolean built;
  String className;

  public CodeBuilder( FragmentContext context ) {
    this.context = context;
    classLoader = new CachedClassLoader( );
  }

  public void setStraightJava( boolean flag ) {
    straightJava = flag;
  }

  public void useCache( boolean flag ) {
    useCache = flag;
  }

//  @SuppressWarnings("unchecked")
//  public Class<T> load( ) throws ClassTransformationException {
//    if ( straightJava ) {
//      // Do something
//      String className = null; // TODO
//      try {
//        return (Class<T>) classLoader.findClass( className );
//      } catch (ClassNotFoundException e) {
//        throw new ClassTransformationException(e);
//      }
//    } else {
//      return (Class<T>) context.getImplementationClass( getCg( ) );
//    }
//  }

  public T newInstance( ) throws ClassTransformationException, IOException, SchemaChangeException {
    if ( straightJava ) {
      try {
        if ( ! built )
          compileClass( );
        @SuppressWarnings("unchecked")
        Class<T> theClass = (Class<T>) classLoader.findClass( className );
        return theClass.newInstance( );
      } catch (ClassNotFoundException e) {
        throw new ClassTransformationException(e);
      } catch (InstantiationException | IllegalAccessException e) {
        throw new ClassTransformationException(e);
      }
    } else {
      try {
        return (T) context.getImplementationClass( getCg( ) );
      } catch (SchemaChangeException e) {
        throw new ClassTransformationException(e);
      }
    }
  }

  private void compileClass() throws IOException, ClassTransformationException, SchemaChangeException {
    getCg( ).generate();
    className = cg.getMaterializedClassName();
    ClassTransformer.ClassNames name = new ClassTransformer.ClassNames( className );
    String code = cg.getGeneratedCode();
    saveCode( code, name );
    classLoader = new CachedClassLoader( );
    ClassCompilerSelector compilerSelector = new ClassCompilerSelector(classLoader, context.getConfig(), getOptions());
    try {
      Map<String,byte[]> results = compilerSelector.compile( name, code );
      classLoader.addClasses( results );
      built = true;
    } catch (CompileException | ClassNotFoundException e) {
      throw new ClassTransformationException(e);
    }
  }

  private void saveCode(String code, ClassNames name) {
    if ( ! saveCode ) { return; }

    String pathName = name.slash + ".java";
    File codeFile = new File( codeDir, pathName );
    codeFile.getParentFile().mkdirs( );
    try (final FileWriter writer = new FileWriter(codeFile) ) {
      writer.write(code);
    } catch (IOException e) {
      System.err.println( "Could not save: " + codeFile.getAbsolutePath() );
    }
  }

  private CodeGenerator<T> getCg( ) throws SchemaChangeException {
    if ( cg == null ) {
      cg = build( );
    }
    return cg;
  }

  protected abstract CodeGenerator<T> build( ) throws SchemaChangeException;

  public boolean isStraightJava() {
    return straightJava;
  }

  public FunctionImplementationRegistry getFunctionRegistry() {
    return context.getFunctionRegistry();
  }

  public OptionManager getOptions() {
    return context.getOptions();
  }
}
