/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl.xsort;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.CodeCompiler;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.ops.CodeGenContext;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.config.ExternalSort;
import org.apache.drill.exec.physical.config.Sort;
import org.apache.drill.exec.physical.impl.xsort.managed.OperatorCodeGenerator;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.server.options.BaseOptionManager;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.server.options.OptionValue.OptionType;
import org.apache.drill.exec.server.options.TypeValidators.BooleanValidator;
import org.apache.drill.exec.server.options.TypeValidators.DoubleValidator;
import org.apache.drill.exec.server.options.TypeValidators.LongValidator;
import org.apache.drill.exec.server.options.TypeValidators.StringValidator;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.IntVector.Mutator;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.test.ConfigBuilder;
import org.junit.Test;

import com.google.common.base.Charsets;

import jersey.repackaged.com.google.common.collect.Lists;

public class TestSortExp {

  @Test
  public void testFieldReference() {
    // Misnomer: the reference must be unquoted.
    FieldReference expr = FieldReference.getWithQuotedRef("foo");
    assertEquals(Types.LATE_BIND_TYPE, expr.getMajorType());
    assertTrue(expr.isSimplePath());
    assertEquals("foo", expr.getRootSegment().getPath());
    assertEquals("`foo`", expr.toExpr());
  }

  @Test
  public void testOrdering() {
    assertEquals(Direction.ASCENDING, Ordering.getOrderingSpecFromString(null));
    assertEquals(Direction.ASCENDING, Ordering.getOrderingSpecFromString(Ordering.ORDER_ASC));
    assertEquals(Direction.DESCENDING, Ordering.getOrderingSpecFromString(Ordering.ORDER_DESC));
    assertEquals(Direction.ASCENDING, Ordering.getOrderingSpecFromString(Ordering.ORDER_ASCENDING));
    assertEquals(Direction.DESCENDING, Ordering.getOrderingSpecFromString(Ordering.ORDER_DESCENDING));
    assertEquals(Direction.ASCENDING, Ordering.getOrderingSpecFromString(Ordering.ORDER_ASC.toLowerCase()));
    assertEquals(Direction.DESCENDING, Ordering.getOrderingSpecFromString(Ordering.ORDER_DESC.toLowerCase()));
    assertEquals(Direction.ASCENDING, Ordering.getOrderingSpecFromString(Ordering.ORDER_ASCENDING.toLowerCase()));
    assertEquals(Direction.DESCENDING, Ordering.getOrderingSpecFromString(Ordering.ORDER_DESCENDING.toLowerCase()));
    try {
      Ordering.getOrderingSpecFromString("");
      fail();
    } catch(DrillRuntimeException e) { }
    try {
      Ordering.getOrderingSpecFromString("foo");
      fail();
    } catch(DrillRuntimeException e) { }

    assertEquals(NullDirection.UNSPECIFIED, Ordering.getNullOrderingFromString(null));
    assertEquals(NullDirection.FIRST, Ordering.getNullOrderingFromString(Ordering.NULLS_FIRST));
    assertEquals(NullDirection.LAST, Ordering.getNullOrderingFromString(Ordering.NULLS_LAST));
    assertEquals(NullDirection.UNSPECIFIED, Ordering.getNullOrderingFromString(Ordering.NULLS_UNSPECIFIED));
    assertEquals(NullDirection.FIRST, Ordering.getNullOrderingFromString(Ordering.NULLS_FIRST.toLowerCase()));
    assertEquals(NullDirection.LAST, Ordering.getNullOrderingFromString(Ordering.NULLS_LAST.toLowerCase()));
    assertEquals(NullDirection.UNSPECIFIED, Ordering.getNullOrderingFromString(Ordering.NULLS_UNSPECIFIED.toLowerCase()));
    try {
      Ordering.getNullOrderingFromString("");
      fail();
    } catch(DrillRuntimeException e) { }
    try {
      Ordering.getNullOrderingFromString("foo");
      fail();
    } catch(DrillRuntimeException e) { }

    FieldReference expr = FieldReference.getWithQuotedRef("foo");

    // Test all getters

    Ordering ordering = new Ordering((String) null, expr, (String) null);
    assertEquals(Direction.ASCENDING, ordering.getDirection());
    assertEquals(NullDirection.UNSPECIFIED, ordering.getNullDirection());
    assertSame(expr, ordering.getExpr());
    assertTrue(ordering.nullsSortHigh());

    // Test all ordering strings

    ordering = new Ordering((String) Ordering.ORDER_ASC, expr, (String) null);
    assertEquals(Direction.ASCENDING, ordering.getDirection());
    assertEquals(NullDirection.UNSPECIFIED, ordering.getNullDirection());

    ordering = new Ordering((String) Ordering.ORDER_ASC.toLowerCase(), expr, (String) null);
    assertEquals(Direction.ASCENDING, ordering.getDirection());
    assertEquals(NullDirection.UNSPECIFIED, ordering.getNullDirection());

    ordering = new Ordering((String) Ordering.ORDER_ASCENDING, expr, (String) null);
    assertEquals(Direction.ASCENDING, ordering.getDirection());
    assertEquals(NullDirection.UNSPECIFIED, ordering.getNullDirection());

    ordering = new Ordering((String) Ordering.ORDER_DESC, expr, (String) null);
    assertEquals(Direction.DESCENDING, ordering.getDirection());
    assertEquals(NullDirection.UNSPECIFIED, ordering.getNullDirection());

    ordering = new Ordering((String) Ordering.ORDER_DESCENDING, expr, (String) null);
    assertEquals(Direction.DESCENDING, ordering.getDirection());
    assertEquals(NullDirection.UNSPECIFIED, ordering.getNullDirection());

    // Test all null ordering strings

    ordering = new Ordering((String) null, expr, Ordering.NULLS_FIRST);
    assertEquals(Direction.ASCENDING, ordering.getDirection());
    assertEquals(NullDirection.FIRST, ordering.getNullDirection());
    assertFalse(ordering.nullsSortHigh());

    ordering = new Ordering((String) null, expr, Ordering.NULLS_FIRST);
    assertEquals(Direction.ASCENDING, ordering.getDirection());
    assertEquals(NullDirection.FIRST, ordering.getNullDirection());
    assertFalse(ordering.nullsSortHigh());

    ordering = new Ordering((String) null, expr, Ordering.NULLS_LAST);
    assertEquals(Direction.ASCENDING, ordering.getDirection());
    assertEquals(NullDirection.LAST, ordering.getNullDirection());
    assertTrue(ordering.nullsSortHigh());

    ordering = new Ordering((String) null, expr, Ordering.NULLS_UNSPECIFIED);
    assertEquals(Direction.ASCENDING, ordering.getDirection());
    assertEquals(NullDirection.UNSPECIFIED, ordering.getNullDirection());
    assertTrue(ordering.nullsSortHigh());

    // Unspecified order is always nulls high

    ordering = new Ordering(Ordering.ORDER_DESC, expr, Ordering.NULLS_UNSPECIFIED);
    assertEquals(Direction.DESCENDING, ordering.getDirection());
    assertEquals(NullDirection.UNSPECIFIED, ordering.getNullDirection());
    assertTrue(ordering.nullsSortHigh());

    // Null sort direction reverses with a Desc sort.

    ordering = new Ordering(Ordering.ORDER_DESC, expr, Ordering.NULLS_FIRST);
    assertEquals(Direction.DESCENDING, ordering.getDirection());
    assertEquals(NullDirection.FIRST, ordering.getNullDirection());
    assertTrue(ordering.nullsSortHigh());

    ordering = new Ordering(Ordering.ORDER_DESC, expr, Ordering.NULLS_LAST);
    assertEquals(Direction.DESCENDING, ordering.getDirection());
    assertEquals(NullDirection.LAST, ordering.getNullDirection());
    assertFalse(ordering.nullsSortHigh());
  }

  @Test
  public void testSortSpec() {
    FieldReference expr = FieldReference.getWithQuotedRef("foo");
    Ordering ordering = new Ordering(Ordering.ORDER_ASC, expr, Ordering.NULLS_FIRST);

    // Basics

    ExternalSort popConfig = new ExternalSort(null, Lists.newArrayList(ordering), false);
    assertSame(ordering, popConfig.getOrderings().get(0));
    assertFalse(popConfig.getReverse());
    assertEquals(SelectionVectorMode.FOUR_BYTE, popConfig.getSVMode());
    assertEquals(CoreOperatorType.EXTERNAL_SORT_VALUE, popConfig.getOperatorType());
    assertEquals(ExternalSort.DEFAULT_INIT_ALLOCATION, popConfig.getInitialAllocation());
    assertEquals(AbstractBase.DEFAULT_MAX_ALLOCATION, popConfig.getMaxAllocation());
    assertTrue(popConfig.isExecutable());

    // Non-default settings

    popConfig = new ExternalSort(null, Lists.newArrayList(ordering), true);
    assertTrue(popConfig.getReverse());
    long maxAlloc = 50_000_000;
    popConfig.setMaxAllocation(maxAlloc);
    assertEquals(ExternalSort.DEFAULT_INIT_ALLOCATION, popConfig.getInitialAllocation());
    assertEquals(maxAlloc, popConfig.getMaxAllocation());
  }

  public static final long ONE_MEG = 1024 * 1024;

  public static class TestOptionSet extends BaseOptionManager {

    private Map<String,OptionValue> values = new HashMap<>();

    public TestOptionSet() {
      // Crashes in FunctionImplementationRegistry if not set
      set(ExecConstants.CAST_TO_NULLABLE_NUMERIC, false);
//      set(ExecConstants.CODE_GEN_EXP_IN_METHOD_SIZE_VALIDATOR, false);
    }

    public void set(String key, int value) {
      set(key, (long) value);
    }

    public void set(String key, long value) {
      values.put(key, OptionValue.createLong(OptionType.SYSTEM, key, value));
    }

    public void set(String key, boolean value) {
      values.put(key, OptionValue.createBoolean(OptionType.SYSTEM, key, value));
    }

    public void set(String key, double value) {
      values.put(key, OptionValue.createDouble(OptionType.SYSTEM, key, value));
    }

    public void set(String key, String value) {
      values.put(key, OptionValue.createString(OptionType.SYSTEM, key, value));
    }

    @Override
    public OptionValue getOption(String name) {
      return values.get(name);
    }
  }

  public static class TestCodeGenContext implements CodeGenContext {

    private final OptionSet options;
    private final CodeCompiler compiler;
    private final FunctionImplementationRegistry functionRegistry;

    public TestCodeGenContext(DrillConfig config, OptionSet options) {
      this.options = options;
      ScanResult classpathScan = ClassPathScanner.fromPrescan(config);
      functionRegistry = new FunctionImplementationRegistry(config, classpathScan, options);
      compiler = new CodeCompiler(config, options);
    }

    @Override
    public FunctionImplementationRegistry getFunctionRegistry() {
      return functionRegistry;
    }

    @Override
    public OptionSet getOptionSet() {
      return options;
    }

    @Override
    public <T> T getImplementationClass(final ClassGenerator<T> cg)
        throws ClassTransformationException, IOException {
      return getImplementationClass(cg.getCodeGenerator());
    }

    @Override
    public <T> T getImplementationClass(final CodeGenerator<T> cg)
        throws ClassTransformationException, IOException {
      return compiler.createInstance(cg);
    }

    @Override
    public <T> List<T> getImplementationClass(final ClassGenerator<T> cg, final int instanceCount) throws ClassTransformationException, IOException {
      return getImplementationClass(cg.getCodeGenerator(), instanceCount);
    }

    @Override
    public <T> List<T> getImplementationClass(final CodeGenerator<T> cg, final int instanceCount) throws ClassTransformationException, IOException {
      return compiler.createInstances(cg, instanceCount);
    }
  }

  public static class TestSchema {

    List<MaterializedField> columns = new ArrayList<>( );

    public TestSchema(List<MaterializedField> columns) {
      this.columns.addAll( columns );
    }

    public int count( ) { return columns.size(); }
    public MaterializedField get(int i) { return columns.get(i); }

  }

  public static class TestSchemaBuilder {
    List<MaterializedField> columns = new ArrayList<>( );

    public TestSchemaBuilder() {

    }

    public TestSchemaBuilder(TestSchema base) {
      columns.addAll(base.columns);
    }

    public TestSchemaBuilder add(String pathName, MajorType type) {
      MaterializedField col = MaterializedField.create(pathName, type);
      columns.add(col);
      return this;
    }

    public TestSchemaBuilder add(String pathName, MinorType type, DataMode mode) {
      return add(pathName, MajorType.newBuilder()
          .setMinorType(type)
          .setMode(mode)
          .build()
          );
    }

    public TestSchemaBuilder add(String pathName, MinorType type) {
      return add(pathName, type, DataMode.REQUIRED);
    }

    public TestSchemaBuilder addNullable(String pathName, MinorType type) {
      return add(pathName, type, DataMode.OPTIONAL);
    }

    public TestSchema build() {
      return new TestSchema(columns);
    }

  }
  public static class TestRecordSet {

    private BufferAllocator allocator;
    private SelectionVector2 sv2;
    private TestSchema schema;
    private ValueVector[] valueVectors;
    private final VectorContainer container = new VectorContainer();
    private SchemaChangeCallBack callBack = new SchemaChangeCallBack();

    public TestRecordSet(BufferAllocator allocator, TestSchema schema) {
      this.allocator = allocator;
      this.schema = schema;
      create();
    }

    private void create() {
      valueVectors = new ValueVector[schema.count()];
      for (int i = 0; i < schema.count(); i++) {
        final MaterializedField field = schema.get(i);
        @SuppressWarnings("resource")
        ValueVector v = TypeHelper.getNewVector(field, allocator, callBack);
        valueVectors[i] = v;
        container.add(v);
      }
      container.buildSchema(SelectionVectorMode.NONE);
    }

    public VectorAccessible getVectorAccessible() {
      return container;
    }

    public void makeSv2() {
      if (sv2 != null) {
        return;
      }
      sv2 = new SelectionVector2(allocator);
      if (!sv2.allocateNewSafe(rowCount())) {
        throw new OutOfMemoryException("Unable to allocate sv2 buffer");
      }
      for (int i = 0; i < rowCount(); i++) {
        sv2.setIndex(i, (char) i);
      }
      sv2.setRecordCount(rowCount());
    }

    public SelectionVector2 getSv2() {
      return sv2;
    }

    public void allocate(int recordCount) {
      for (final ValueVector v : valueVectors) {
        AllocationHelper.allocate(v, recordCount, 50, 10);
      }
    }

    public void setRowCount(int rowCount) {
      container.setRecordCount(rowCount);
      for (VectorWrapper<?> w : container) {
        w.getValueVector().getMutator().setValueCount(rowCount);
      }
    }

    public int rowCount() { return container.getRecordCount(); }

    RecordSetWriter writer() {
      return new RecordSetWriterImpl(this);
    }

    RecordSetReader reader() {
      return new RecordSetReaderImpl(this);
    }

    public void clear() {
      container.zeroVectors();
      if (sv2 != null) {
        sv2.clear();
      }
      container.setRecordCount(0);
      sv2 = null;
    }
  }

  public interface ColumnWriter {
    void setNull();
    void setInt(int value);
    void setString(String value);
  }

  public interface ColumnReader {
    boolean isNull();
    int getInt();
    String getString();
  }

  public interface RowIndex {
    int getRow();
  }

  public static abstract class ColumnAccessor {
    private RowIndex rowIndex;

    protected void bind(RowIndex rowIndex) {
      this.rowIndex = rowIndex;
    }

    protected abstract void bind(RowIndex rowIndex, ValueVector vector);

    protected int rowIndex() { return rowIndex.getRow(); }

  }

  public static abstract class AbstractColumnWriter extends ColumnAccessor implements ColumnWriter {

    @Override
    public void setNull() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setInt(int value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setString(String value) {
      throw new UnsupportedOperationException();
    }
  }

  public static abstract class AbstractColumnReader extends ColumnAccessor implements ColumnReader {

    @Override
    public boolean isNull() {
      return false;
    }

    @Override
    public int getInt() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getString() {
      throw new UnsupportedOperationException();
    }
  }

  public static class IntColumnWriter extends AbstractColumnWriter {

    private IntVector.Mutator mutator;

    @Override
    protected void bind(RowIndex rowIndex, ValueVector vector) {
      bind(rowIndex);
      this.mutator = ((IntVector) vector).getMutator();
    }

    @Override
    public void setInt(int value) {
      mutator.setSafe(rowIndex(), value);
    }
  }

  public static class VarCharColumnWriter extends AbstractColumnWriter {

    private VarCharVector.Mutator mutator;

    @Override
    protected void bind(RowIndex rowIndex, ValueVector vector) {
      bind(rowIndex);
      this.mutator = ((VarCharVector) vector).getMutator();
    }

    @Override
    public void setString(String value) {
      mutator.setSafe(rowIndex(), value.getBytes());
    }
  }

  public static class IntColumnReader extends AbstractColumnReader {

    private IntVector.Accessor accessor;

    @Override
    protected void bind(RowIndex rowIndex, ValueVector vector) {
      bind(rowIndex);
      this.accessor = ((IntVector) vector).getAccessor();
    }

    @Override
    public int getInt() {
      return accessor.get(rowIndex());
    }
  }

  public static class VarCharColumnReader extends AbstractColumnReader {

    private VarCharVector.Accessor accessor;

    @Override
    protected void bind(RowIndex rowIndex, ValueVector vector) {
      bind(rowIndex);
      this.accessor = ((VarCharVector) vector).getAccessor();
    }

    @Override
    public String getString() {
      return new String(accessor.get(rowIndex()), Charsets.UTF_8);
    }
  }

  @SuppressWarnings("unchecked")
  public static class ColumnAccessorFactory {

    private static Class<? extends AbstractColumnWriter> writers[][] = buildWriters();
    private static Class<? extends AbstractColumnReader> readers[][] = buildReaders();

    private static Class<? extends AbstractColumnWriter>[][] buildWriters() {
      int typeCount = MinorType.values().length;
      int modeCount = DataMode.values().length;
      Class<? extends AbstractColumnWriter> writers[][] = new Class[typeCount][];
      for (int i = 0; i < typeCount; i++) {
        writers[i] = new Class[modeCount];
      }

      writers[MinorType.INT.ordinal()][DataMode.REQUIRED.ordinal()] = IntColumnWriter.class;
      writers[MinorType.VARCHAR.ordinal()][DataMode.REQUIRED.ordinal()] = VarCharColumnWriter.class;
      return writers;
    }

    private static Class<? extends AbstractColumnReader>[][] buildReaders() {
      int typeCount = MinorType.values().length;
      int modeCount = DataMode.values().length;
      Class<? extends AbstractColumnReader> readers[][] = new Class[typeCount][];
      for (int i = 0; i < typeCount; i++) {
        readers[i] = new Class[modeCount];
      }

      readers[MinorType.INT.ordinal()][DataMode.REQUIRED.ordinal()] = IntColumnReader.class;
      readers[MinorType.VARCHAR.ordinal()][DataMode.REQUIRED.ordinal()] = VarCharColumnReader.class;
      return readers;
    }

    public static AbstractColumnWriter newWriter(MajorType type) {
      Class<? extends AbstractColumnWriter> writerClass = writers[type.getMinorType().ordinal()][type.getMode().ordinal()];
      if (writerClass == null) {
        throw new UnsupportedOperationException();
      }
      try {
        return writerClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new IllegalStateException(e);
      }
    }

    public static AbstractColumnReader newReader(MajorType type) {
      Class<? extends AbstractColumnReader> writerClass = readers[type.getMinorType().ordinal()][type.getMode().ordinal()];
      if (writerClass == null) {
        throw new UnsupportedOperationException();
      }
      try {
        return writerClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  public interface RecordSetWriter {
    void advance();
    void done();
    ColumnWriter column(int colIndex);
    ColumnWriter column(String colName);
  }

  public interface RecordSetReader {
    boolean valid();
    boolean advance();
    int rowIndex();
    int rowCount();
    ColumnReader column(int colIndex);
    ColumnReader column(String colName);
  }

  public static class RecordSetWriterImpl implements RecordSetWriter, RowIndex {

    private TestRecordSet recordSet;
    private AbstractColumnWriter writers[];
    private int rowIndex;

    public RecordSetWriterImpl(TestRecordSet recordSet) {
      this.recordSet = recordSet;
      ValueVector[] valueVectors = recordSet.valueVectors;
      writers = new AbstractColumnWriter[valueVectors.length];
      for (int i = 0; i < writers.length; i++) {
        writers[i] = ColumnAccessorFactory.newWriter(valueVectors[i].getField().getType());
        writers[i].bind(this, valueVectors[i]);
      }
    }

    @Override
    public int getRow() {
      return rowIndex;
    }

    @Override
    public void advance() {
      rowIndex++;
    }

    @Override
    public ColumnWriter column(int colIndex) {
      return writers[colIndex];
    }

    @Override
    public ColumnWriter column(String colName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void done() {
      recordSet.setRowCount(rowIndex);
    }

  }

  public static abstract class AbstractRowIndex implements RowIndex {
    protected int rowIndex;

    public void advance() { rowIndex++; }
    public int getIndex() { return rowIndex; }
  }

  public static class DirectRowIndex extends AbstractRowIndex {

    @Override
    public int getRow() {
      return rowIndex;
    }
  }

  public static class Sv2RowIndex extends AbstractRowIndex {

    private SelectionVector2 sv2;

    public Sv2RowIndex(SelectionVector2 sv2) {
      this.sv2 = sv2;
    }

    @Override
    public int getRow() {
      return sv2.getIndex(rowIndex);
    }
  }

  public static class RecordSetReaderImpl implements RecordSetReader {

    private TestRecordSet recordSet;
    private AbstractColumnReader readers[];
    private AbstractRowIndex rowIndex;

    public RecordSetReaderImpl(TestRecordSet recordSet) {
      if (recordSet.getSv2() == null) {
        rowIndex = new DirectRowIndex();
      } else {
        rowIndex = new Sv2RowIndex(recordSet.getSv2());
      }
      this.recordSet = recordSet;
      ValueVector[] valueVectors = recordSet.valueVectors;
      readers = new AbstractColumnReader[valueVectors.length];
      for (int i = 0; i < readers.length; i++) {
        readers[i] = ColumnAccessorFactory.newReader(valueVectors[i].getField().getType());
        readers[i].bind(rowIndex, valueVectors[i]);
      }
    }

    @Override
    public boolean valid() {
      return rowIndex.getIndex() < recordSet.rowCount();
    }

    @Override
    public boolean advance() {
      if (rowIndex.getIndex() >= recordSet.rowCount())
        return false;
      rowIndex.advance();
      return valid();
    }

    @Override
    public ColumnReader column(int colIndex) {
      return readers[colIndex];
    }

    @Override
    public ColumnReader column(String colName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int rowIndex() {
      return rowIndex.getIndex();
    }

    @Override
    public int rowCount() {
      return recordSet.rowCount();
    }
  }

  public static class OperatorFixtureBuilder
  {
    ConfigBuilder configBuilder = new ConfigBuilder();
    TestOptionSet options = new TestOptionSet();

    public ConfigBuilder configBuilder() {
      return configBuilder;
    }

    public TestOptionSet options() {
      return options;
    }

    public OperatorFixture build() {
      return new OperatorFixture(this);
    }
  }

  public static class OperatorFixture implements AutoCloseable {

    DrillConfig config;
    BufferAllocator allocator;
    TestOptionSet options;
    TestCodeGenContext context;

    protected OperatorFixture(OperatorFixtureBuilder builder) {
      config = builder.configBuilder().build();
      allocator = RootAllocatorFactory.newRoot(config);
      options = builder.options();
      context = new TestCodeGenContext(config, options);
     }

    public BufferAllocator allocator() { return allocator; }

    public TestOptionSet options() { return options; }


    public CodeGenContext codeGenContext() { return context; }

    @Override
    public void close() throws Exception {
      allocator.close();
    }

    public static OperatorFixtureBuilder builder() {
      return new OperatorFixtureBuilder();
    }

  }

  @Test
  public void testSorter() throws Exception {
    try (OperatorFixture fixture = OperatorFixture.builder().build()) {

      FieldReference expr = FieldReference.getWithQuotedRef("key");
      Ordering ordering = new Ordering(Ordering.ORDER_ASC, expr, Ordering.NULLS_LAST);
      Sort popConfig = new Sort(null, Lists.newArrayList(ordering), false);

      OperatorCodeGenerator opCodeGen = new OperatorCodeGenerator(fixture.codeGenContext(), popConfig);

      TestSchema schema = new TestSchemaBuilder()
          .add("key", MinorType.INT)
          .add("value", MinorType.VARCHAR)
          .build();

      TestRecordSet recSet = new TestRecordSet(fixture.allocator(), schema);
      SingleBatchSorter sorter = opCodeGen.getSorter(recSet.getVectorAccessible());

      // Sort empty row set.

      recSet.allocate(10);
      recSet.setRowCount(0);
      recSet.makeSv2();
      sorter.setup(null, recSet.getSv2(), recSet.getVectorAccessible());
      sorter.sort(recSet.getSv2());
      assertEquals(0, recSet.rowCount());

      // Sort with one row

      recSet.clear();
      recSet.allocate(10);
      RecordSetWriter writer = recSet.writer();
      writer.column(0).setInt(0);
      writer.column(1).setString("0");
      writer.advance();
      writer.done();

      assertEquals(1, recSet.rowCount());

      recSet.makeSv2();
      sorter.setup(null, recSet.getSv2(), recSet.getVectorAccessible());
      sorter.sort(recSet.getSv2());

      RecordSetReader reader = recSet.reader();
      assertTrue(reader.valid());
      assertEquals(0, reader.column(0).getInt());
      assertEquals("0", reader.column(1).getString());
      assertFalse(reader.advance());
      assertFalse(reader.valid());
      recSet.clear();
    }
  }

}
