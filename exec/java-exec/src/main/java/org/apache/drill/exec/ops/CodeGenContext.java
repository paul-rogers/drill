package org.apache.drill.exec.ops;

import java.io.IOException;
import java.util.List;

import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.server.options.OptionSet;

public interface CodeGenContext {
  FunctionImplementationRegistry getFunctionRegistry();
  OptionSet getOptionSet();

  <T> T getImplementationClass(final ClassGenerator<T> cg)
      throws ClassTransformationException, IOException;

  <T> T getImplementationClass(final CodeGenerator<T> cg)
      throws ClassTransformationException, IOException;

  <T> List<T> getImplementationClass(final ClassGenerator<T> cg, final int instanceCount)
      throws ClassTransformationException, IOException;

  <T> List<T> getImplementationClass(final CodeGenerator<T> cg, final int instanceCount)
      throws ClassTransformationException, IOException;
}
