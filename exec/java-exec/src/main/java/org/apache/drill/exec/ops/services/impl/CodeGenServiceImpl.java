package org.apache.drill.exec.ops.services.impl;

import java.io.IOException;
import java.util.List;

import org.apache.drill.exec.compile.CodeCompiler;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.ops.services.CodeGenService;

public class CodeGenServiceImpl implements CodeGenService {

  private final FunctionImplementationRegistry funcRegistry;
  private final CodeCompiler codeCompiler;

  public CodeGenServiceImpl(FunctionImplementationRegistry funcRegistry, CodeCompiler codeCompiler) {
    this.funcRegistry = funcRegistry;
    this.codeCompiler = codeCompiler;
  }

  @Override
  public FunctionImplementationRegistry getFunctionRegistry() {
    return funcRegistry;
  }

  @Override
  public <T> T getImplementationClass(final ClassGenerator<T> cg)
      throws ClassTransformationException, IOException {
    return getImplementationClass(cg.getCodeGenerator());
  }

  @Override
  public <T> T getImplementationClass(final CodeGenerator<T> cg)
      throws ClassTransformationException, IOException {
    return codeCompiler.createInstance(cg);
  }

  @Override
  public <T> List<T> getImplementationClass(final ClassGenerator<T> cg, final int instanceCount)
      throws ClassTransformationException, IOException {
    return getImplementationClass(cg.getCodeGenerator(), instanceCount);
  }

  @Override
  public <T> List<T> getImplementationClass(final CodeGenerator<T> cg, final int instanceCount)
      throws ClassTransformationException, IOException {
    return codeCompiler.createInstances(cg, instanceCount);
  }
}
