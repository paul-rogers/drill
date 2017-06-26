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
package org.apache.drill.exec.physical.rowSet;

import static org.junit.Assert.*;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.Exp4.*;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.IntVector.Accessor;
import org.apache.drill.exec.vector.IntVector.Mutator;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

import io.netty.util.internal.PlatformDependent;

public class TestExp4 extends SubOperatorTest {

  public static class MockIndexer implements RowIndexer {

    public int rowId;

    @Override
    public int rowId() { return rowId; }
  }

  @Test
  public void testInt() {
    PageAllocatorImpl pageAllocator = new PageAllocatorImpl(fixture.allocator());
    MaterializedField schema = SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED);
    MutablePagedValueVector mutable = new MutablePagedValueVector(schema, pageAllocator);
    MockIndexer indexer = new MockIndexer();
    PagedWriter writer = mutable.writer(indexer);
    int rowCount = Exp4.PAGE_SIZE / Exp4.IntIndexer.VALUE_WIDTH * 2;
    for (int i = 0; i < rowCount; i++) {
      indexer.rowId = i;
      writer.setInt(i);
    }

    PagedValueVector immutable = new ImmutablePagedValueVector(mutable);
    PagedReader reader = immutable.reader(indexer);
    for (int i = 0; i < rowCount; i++) {
      indexer.rowId = i;
      assertEquals(i, reader.getInt());
    }

    immutable.free();
    immutable.free();
    pageAllocator.close();
  }

  public static final int TARGET_INT_COUNT = 32 * 1024 * 1024 / 4;
  public static final int REP_COUNT = 100;

  @Test
  public void timeInt() {
    // Warm up
    timeExistingVector();
    timeNewVector();
    // Test
    timeExistingVector();
    timeNewVector();
  }

  private void timeExistingVector() {
    MaterializedField schema = SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED);
    long start = System.currentTimeMillis();
    for (int rep = 0; rep < REP_COUNT; rep++) {
      try (IntVector vector = new IntVector(schema, fixture.allocator())) {
        vector.allocateNew(1024);
        Mutator mutator = vector.getMutator();
        for (int i = 0; i < TARGET_INT_COUNT; i++) {
          mutator.setSafe(i, i);
        }
        Accessor accessor = vector.getAccessor();
        for (int i = 0; i < TARGET_INT_COUNT; i++) {
          accessor.get(i);
        }
      }
    }
    long duration = System.currentTimeMillis() - start;
    System.out.println("Existing vector: " + duration + " ms.");
  }

  private void timeNewVector() {
    PageAllocatorImpl pageAllocator = new PageAllocatorImpl(fixture.allocator());
    MaterializedField schema = SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED);
    long start = System.currentTimeMillis();
    for (int rep = 0; rep < REP_COUNT; rep++) {
      MutablePagedValueVector mutable = new MutablePagedValueVector(schema, pageAllocator);
      MockIndexer indexer = new MockIndexer();
      PagedWriter writer = mutable.writer(indexer);
      for (int i = 0; i < TARGET_INT_COUNT; i++) {
        indexer.rowId = i;
        writer.setInt(i);
      }
      PagedValueVector immutable = new ImmutablePagedValueVector(mutable);
      PagedReader reader = immutable.reader(indexer);
      for (int i = 0; i < TARGET_INT_COUNT; i++) {
        indexer.rowId = i;
        reader.getInt();
      }
      immutable.free();
    }
    pageAllocator.close();
    long duration = System.currentTimeMillis() - start;
    System.out.println("New vector: " + duration + " ms.");
  }

  private void timeNewVector2() {
    PageAllocatorImpl pageAllocator = new PageAllocatorImpl(fixture.allocator());
    MaterializedField schema = SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED);
    long start = System.currentTimeMillis();
    MutablePagedValueVector mutable = new MutablePagedValueVector(schema, pageAllocator);
    for (int rep = 0; rep < REP_COUNT; rep++) {
      MockIndexer indexer = new MockIndexer();
      PagedWriter writer = mutable.writer(indexer);
      for (int i = 0; i < TARGET_INT_COUNT; i++) {
        indexer.rowId = i;
        writer.setInt(i);
      }
      PagedValueVector immutable = new ImmutablePagedValueVector(mutable);
      PagedReader reader = immutable.reader(indexer);
      for (int i = 0; i < TARGET_INT_COUNT; i++) {
        indexer.rowId = i;
        reader.getInt();
      }
    }
    mutable.free();
    pageAllocator.close();
    long duration = System.currentTimeMillis() - start;
    System.out.println("timeNewVector2: " + duration + " ms.");
  }

  @Test
  public void timeInt2() {
    for (int i = 0; i < 2; i++) {
//      timeNewVector();
//      timeNewVector2();
//      timeNewVector3();
//      timeNewVector4();
      timeNewVector5();
//      timeNewVector6();
//      timeNewVector7();
//      timeNewVector8();
      timeNewVector9();
    }
  }

  public static void main(String args[]) throws Exception {
    TestExp4.setUpBeforeClass();
    new TestExp4().timeInt2();
  }

  private void timeNewVector3() {
    PageAllocatorImpl pageAllocator = new PageAllocatorImpl(fixture.allocator());
    MaterializedField schema = SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED);
    long start = System.currentTimeMillis();
    MutablePagedValueVector mutable = new MutablePagedValueVector(schema, pageAllocator);
    long addr = mutable.page(0).addr();
    long sum = 0;
    for (int rep = 0; rep < REP_COUNT; rep++) {
      for (int i = 0; i < TARGET_INT_COUNT; i++) {
        PlatformDependent.putInt(addr, i);
      }
      for (int i = 0; i < TARGET_INT_COUNT; i++) {
        sum += PlatformDependent.getInt(addr);
      }
    }
    mutable.free();
    pageAllocator.close();
    long duration = System.currentTimeMillis() - start;
    System.out.println("timeNewVector3: " + duration + " ms.");
    System.out.println(sum);
  }

  private void timeNewVector4() {
    PageAllocatorImpl pageAllocator = new PageAllocatorImpl(fixture.allocator());
    MaterializedField schema = SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED);
    long start = System.currentTimeMillis();
    long sum = 0;
    for (int rep = 0; rep < REP_COUNT; rep++) {
      MutablePagedValueVector mutable = new MutablePagedValueVector(schema, pageAllocator);
      for (int i = 0; i < 32; i++) {
        sum += mutable.page(i).addr();
      }
      PagedValueVector immutable = new ImmutablePagedValueVector(mutable);
      for (int i = 0; i < 32; i++) {
        sum += immutable.page(i).addr();
      }
      immutable.free();
    }
    pageAllocator.close();
    long duration = System.currentTimeMillis() - start;
    System.out.println("timeNewVector4: " + duration + " ms.");
    System.out.println(sum);
  }

  private void timeNewVector5() {
    PageAllocatorImpl pageAllocator = new PageAllocatorImpl(fixture.allocator());
    MaterializedField schema = SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED);
    long start = System.currentTimeMillis();
    long sum = 0;
    for (int rep = 0; rep < REP_COUNT; rep++) {
      MutablePagedValueVector mutable = new MutablePagedValueVector(schema, pageAllocator);
      int rowId = 0;
      for (int i = 0; i < 32; i++) {
        long addr = mutable.page(i).addr();
        for (int j = 0; j < IntConstants.VALUES_PER_PAGE; j++, rowId++) {
          PlatformDependent.putInt(addr, rowId);
          addr = Exp4.foo(addr);
//          addr += IntConstants.VALUE_WIDTH;
        }
      }
      PagedValueVector immutable = new ImmutablePagedValueVector(mutable);
      rowId = 0;
      for (int i = 0; i < 32; i++) {
        long addr = mutable.page(i).addr();
        for (int j = 0; j < IntConstants.VALUES_PER_PAGE; j++, rowId++) {
          sum += PlatformDependent.getInt(addr);
          addr = Exp4.foo(addr);
//          addr += IntConstants.VALUE_WIDTH;
        }
      }
      immutable.free();
      assert rowId == TARGET_INT_COUNT;
    }
    pageAllocator.close();
    long duration = System.currentTimeMillis() - start;
    System.out.println("timeNewVector5: " + duration + " ms.");
    System.out.println(sum);
  }

  private void timeNewVector6() {
    PageAllocatorImpl pageAllocator = new PageAllocatorImpl(fixture.allocator());
    MaterializedField schema = SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED);
    long start = System.currentTimeMillis();
    for (int rep = 0; rep < REP_COUNT; rep++) {
      MutablePagedValueVector mutable = new MutablePagedValueVector(schema, pageAllocator);
      int rowId = 0;
      for (int i = 0; i < 32; i++) {
        long addr = mutable.page(i).addr();
        for (int j = 0; j < IntConstants.VALUES_PER_PAGE; j++, rowId++) {
          addr += IntConstants.VALUE_WIDTH;
        }
        assert addr > 0;
      }
      PagedValueVector immutable = new ImmutablePagedValueVector(mutable);
      rowId = 0;
      for (int i = 0; i < 32; i++) {
        long addr = mutable.page(i).addr();
        for (int j = 0; j < IntConstants.VALUES_PER_PAGE; j++, rowId++) {
          addr += IntConstants.VALUE_WIDTH;
        }
        assert addr > 0;
      }
      immutable.free();
    }
    pageAllocator.close();
    long duration = System.currentTimeMillis() - start;
    System.out.println("timeNewVector6: " + duration + " ms.");
  }

  private void timeNewVector7() {
    long start = System.currentTimeMillis();
    byte buf[] = new byte[1024];
    int valuesPerBuf = buf.length / IntConstants.VALUE_WIDTH;
    int bufsPerPage = Exp4.PAGE_SIZE / buf.length;
    long sum = 0;
    for (int rep = 0; rep < REP_COUNT; rep++) {
      int rowId = 0;
      for (int i = 0; i < 32; i++) {
        for (int j = 0; j < bufsPerPage; j++) {
          for (int k = 0; k < valuesPerBuf; rowId++) {
            buf[k++] = (byte) (rowId & 0xFF);
            int value = rowId >>> 8;
            buf[k] = (byte) (value & 0xFF);
            value = value >>> 8;
            buf[k] = (byte) (value & 0xFF);
            value = value >>> 8;
            buf[k] = (byte) (value & 0xFF);
          }
        }
      }
      rowId = 0;
      for (int i = 0; i < 32; i++) {
        for (int j = 0; j < bufsPerPage; j++) {
          for (int k = 0; k < valuesPerBuf; rowId++) {
            int value = buf[k++] +
                        buf[k++] << 8 +
                        buf[k++] << 16 +
                        buf[k++] << 24;
            sum += value;
          }
        }
      }
    }
    long duration = System.currentTimeMillis() - start;
    System.out.println("timeNewVector7: " + duration + " ms.");
    System.out.println(sum);
  }

  private void timeNewVector8() {
    PageAllocatorImpl pageAllocator = new PageAllocatorImpl(fixture.allocator());
    MaterializedField schema = SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED);
    long start = System.currentTimeMillis();
    long sum = 0;
    for (int rep = 0; rep < REP_COUNT; rep++) {
      MutablePagedValueVector mutable = new MutablePagedValueVector(schema, pageAllocator);
      long addr = 0;
      int curPageId = -1;
      for (int i = 0; i < TARGET_INT_COUNT; i++) {
        int pageId = i / IntConstants.VALUES_PER_PAGE;
        if (pageId != curPageId) {
          addr = mutable.page(pageId).addr();
          curPageId = pageId;
        }
        PlatformDependent.putInt(addr + (i % IntConstants.VALUES_PER_PAGE) * IntConstants.VALUE_WIDTH, i);
      }
      PagedValueVector immutable = new ImmutablePagedValueVector(mutable);
      for (int i = 0; i < TARGET_INT_COUNT; i++) {
        int pageId = i / IntConstants.VALUES_PER_PAGE;
        if (pageId != curPageId) {
          addr = mutable.page(pageId).addr();
          curPageId = pageId;
        }
        sum += PlatformDependent.getInt(addr + (i % IntConstants.VALUES_PER_PAGE) * IntConstants.VALUE_WIDTH);
      }
      immutable.free();
    }
    pageAllocator.close();
    long duration = System.currentTimeMillis() - start;
    System.out.println("timeNewVector8: " + duration + " ms.");
    System.out.println(sum);
  }


  private void timeNewVector9() {
    PageAllocatorImpl pageAllocator = new PageAllocatorImpl(fixture.allocator());
    MaterializedField schema = SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED);
    long start = System.currentTimeMillis();
    long sum = 0;
    for (int rep = 0; rep < REP_COUNT; rep++) {
      MutablePagedValueVector mutable = new MutablePagedValueVector(schema, pageAllocator);
      long addr = 0;
      long endAddr = 0;
      int prevRowId = -2;
      for (int i = 0; i < TARGET_INT_COUNT; i++) {
        addr += IntConstants.VALUE_WIDTH;
        if (i != prevRowId + 1 || addr >= endAddr) {
          int pageId = i / IntConstants.VALUES_PER_PAGE;
          addr = mutable.page(pageId).addr();
          endAddr = addr + Exp4.PAGE_SIZE;
          addr += (i % IntConstants.VALUES_PER_PAGE) * IntConstants.VALUE_WIDTH;
        }
        PlatformDependent.putInt(addr, i);
        prevRowId = i;
      }
      PagedValueVector immutable = new ImmutablePagedValueVector(mutable);
      addr = 0;
      endAddr = 0;
      prevRowId = -2;
      for (int i = 0; i < TARGET_INT_COUNT; i++) {
        addr += IntConstants.VALUE_WIDTH;
        if (i != prevRowId + 1 || addr >= endAddr) {
          int pageId = i / IntConstants.VALUES_PER_PAGE;
          addr = immutable.page(pageId).addr();
          endAddr = addr + Exp4.PAGE_SIZE;
          addr += (i % IntConstants.VALUES_PER_PAGE) * IntConstants.VALUE_WIDTH;
        }
        sum += PlatformDependent.getInt(addr);
        prevRowId = i;
      }
      immutable.free();
    }
    pageAllocator.close();
    long duration = System.currentTimeMillis() - start;
    System.out.println("timeNewVector8: " + duration + " ms.");
    System.out.println(sum);
  }

}
