package org.apache.drill.exec.physical.impl.xsort.managed;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.ExternalSort;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.physical.impl.sort.SortRecordBatchBuilder;
import org.apache.drill.exec.physical.impl.xsort.MSorter;
import org.apache.drill.exec.physical.impl.xsort.managed.ExternalSortBatch.SortResults;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.selection.SelectionVector4;

import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;

public class InMemoryMerge implements SortResults {

  private SortRecordBatchBuilder builder;
  private MSorter mSorter;
  private final FragmentContext context;
  private final BufferAllocator oAllocator;
  private SelectionVector4 sv4;
  private final ExternalSort popConfig;

  public InMemoryMerge( FragmentContext context, BufferAllocator allocator, ExternalSort popConfig ) {
    this.context = context;
    this.oAllocator = allocator;
    this.popConfig = popConfig;
  }

  public SelectionVector4 merge( LinkedList<BatchGroup.InputBatchGroup> batchGroups, VectorAccessible batch, VectorContainer destContainer ) throws SchemaChangeException, ClassTransformationException, IOException {
    if (builder != null) {
      builder.clear();
      builder.close();
    }
    builder = new SortRecordBatchBuilder(oAllocator);

    while ( ! batchGroups.isEmpty() ) {
      BatchGroup.InputBatchGroup group = batchGroups.pollLast();
      RecordBatchData rbd = new RecordBatchData(group.getContainer(), oAllocator);
      rbd.setSv2(group.getSv2());
      builder.add(rbd);
    }

    builder.build(context, destContainer);
    sv4 = builder.getSv4();
    mSorter = createNewMSorter( batch );
    mSorter.setup(context, oAllocator, sv4, destContainer);

    // For testing memory-leak purpose, inject exception after mSorter finishes setup
    ExternalSortBatch.injector.injectUnchecked(context.getExecutionControls(), ExternalSortBatch.INTERRUPTION_AFTER_SETUP);
    mSorter.sort(destContainer);

    // sort may have prematurely exited due to should continue returning false.
    if (!context.shouldContinue()) {
      return null;
    }

    // For testing memory-leak purpose, inject exception after mSorter finishes sorting
    ExternalSortBatch.injector.injectUnchecked(context.getExecutionControls(), ExternalSortBatch.INTERRUPTION_AFTER_SORT);
    sv4 = mSorter.getSV4();

    destContainer.buildSchema(SelectionVectorMode.FOUR_BYTE);
    return sv4;
  }

  @Override
  public boolean next() {
    return sv4.next();
  }

  private MSorter createNewMSorter( VectorAccessible batch ) throws ClassTransformationException, IOException, SchemaChangeException {
    return createNewMSorter(context, popConfig.getOrderings(), batch, ExternalSortBatch.MAIN_MAPPING, ExternalSortBatch.LEFT_MAPPING, ExternalSortBatch.RIGHT_MAPPING);
  }

  private MSorter createNewMSorter(FragmentContext context, List<Ordering> orderings, VectorAccessible batch, MappingSet mainMapping, MappingSet leftMapping, MappingSet rightMapping)
          throws ClassTransformationException, IOException, SchemaChangeException{
    CodeGenerator<MSorter> cg = CodeGenerator.get(MSorter.TEMPLATE_DEFINITION, context.getFunctionRegistry(), context.getOptions());
    ClassGenerator<MSorter> g = cg.getRoot();
    g.setMappingSet(mainMapping);

    for (Ordering od : orderings) {
      // first, we rewrite the evaluation stack for each side of the comparison.
      ErrorCollector collector = new ErrorCollectorImpl();
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(od.getExpr(), batch, collector, context.getFunctionRegistry());
      if (collector.hasErrors()) {
        throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
      }
      g.setMappingSet(leftMapping);
      HoldingContainer left = g.addExpr(expr, ClassGenerator.BlkCreateMode.FALSE);
      g.setMappingSet(rightMapping);
      HoldingContainer right = g.addExpr(expr, ClassGenerator.BlkCreateMode.FALSE);
      g.setMappingSet(mainMapping);

      // next we wrap the two comparison sides and add the expression block for the comparison.
      LogicalExpression fh =
          FunctionGenerationHelper.getOrderingComparator(od.nullsSortHigh(), left, right,
                                                         context.getFunctionRegistry());
      HoldingContainer out = g.addExpr(fh, ClassGenerator.BlkCreateMode.FALSE);
      JConditional jc = g.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));

      if (od.getDirection() == Direction.ASCENDING) {
        jc._then()._return(out.getValue());
      }else{
        jc._then()._return(out.getValue().minus());
      }
      g.rotateBlock();
    }

    g.rotateBlock();
    g.getEvalBlock()._return(JExpr.lit(0));

    return context.getImplementationClass(cg);
  }

  @Override
  public void close() {
    if (builder != null) {
      builder.clear();
      builder.close();
    }
    if (mSorter != null) {
      mSorter.clear();
    }
  }
}