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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.MaterializedField;

import io.netty.buffer.DrillBuf;
import io.netty.util.internal.PlatformDependent;

public class Exp4 {

//  public static final int PAGE_SIZE = 4096;
  public static final int PAGE_SIZE = 1024 * 1024;

  public static interface RowIndexer {
    int rowId();
  }

  public interface PagedValueVector {
    MaterializedField schema();
    PagedWriter writer(RowIndexer rowIndexer);
    PagedReader reader(RowIndexer rowIndexer);
    int pageCount();
    Page page(int pageId);
    void free();
  }

  public interface PagedReader {
    boolean isNull();
    int getInt();
    int getLong();
    String getString();
  }

  public interface PagedWriter {
    void setNull(boolean isNull);
    void setInt(int value);
    void setLong(long value);
    void setString(String value);
    void copyFrom(PagedReader reader);
  }

  public interface PageAllocator {
    Page alloc();
    void free(Page page);
  }

  public interface Page {
    long addr();
  }

  public static abstract class AbstractPagedValueVector implements PagedValueVector {
    protected final MaterializedField schema;
    protected final PageAllocator allocator;

    public AbstractPagedValueVector(MaterializedField schema, PageAllocator allocator) {
      this.schema = schema;
      this.allocator = allocator;
    }

    @Override
    public MaterializedField schema() { return schema; }

    @Override
    public PagedReader reader(RowIndexer rowIndexer) {
      PagedReaderImpl writer = PagedVectorAccessorFactory.newReader(schema.getType());
      writer.bind(this, rowIndexer);
      return writer;
    }
  }

  public static class MutablePagedValueVector extends AbstractPagedValueVector {
    private final List<Page> pages = new ArrayList<>();

    public MutablePagedValueVector(MaterializedField schema, PageAllocator allocator) {
      super(schema, allocator);
    }

    @Override
    public int pageCount() { return pages.size(); }

    @Override
    public Page page(int pageIndex) {
      if (pageIndex < pages.size()) {
        return pages.get(pageIndex);
      } else {
        assert pageIndex == pages.size();
        Page page = allocator.alloc();
        pages.add(page);
        return page;
      }
    }

    @Override
    public PagedWriter writer(RowIndexer rowIndexer) {
      PagedWriterImpl writer = PagedVectorAccessorFactory.newWriter(schema.getType());
      writer.bind(this, rowIndexer);
      return writer;
    }

    @Override
    public void free() {
      for (int i = 0; i < pages.size(); i++) {
        allocator.free(pages.get(i));
      }
      pages.clear();
    }
  }

  public static class ImmutablePagedValueVector extends AbstractPagedValueVector {
    private final Page pages[];

    public ImmutablePagedValueVector(MutablePagedValueVector source) {
      super(source.schema, source.allocator);
      pages = new Page[source.pages.size()];
      source.pages.toArray(pages);
    }

    @Override
    public int pageCount() { return pages.length; }

    @Override
    public Page page(int pageIndex) { return pages[pageIndex]; }

    @Override
    public PagedWriter writer(RowIndexer rowIndexer) {
      throw new IllegalStateException("Cannot write to an immutable vector.");
    }

    @Override
    public void free() {
      for (int i = 0; i < pages.length; i++) {
        if (pages[i] != null) {
          allocator.free(pages[i]);
        }
        pages[i] = null;
      }
    }
  }

  public static class PageAllocatorImpl implements PageAllocator {
    private final Queue<PageImpl> freePages = new ArrayDeque<>();
    private final BufferAllocator allocator;

    public PageAllocatorImpl(BufferAllocator allocator) {
      this.allocator = allocator;
    }

    @Override
    public Page alloc() {
      Page page = freePages.poll();
      if (page != null) {
        return page;
      }
      return new PageImpl(allocator);
    }

    @Override
    public void free(Page page) {
      freePages.add((PageImpl) page);
    }

    public void close() {
      PageImpl page;
      while ((page = freePages.poll()) != null) {
        page.free();
      }
    }
  }

  public static class PageImpl implements Page {
    private DrillBuf buffer;
    private long addr;

    public PageImpl(BufferAllocator allocator) {
      buffer = allocator.buffer(PAGE_SIZE);
      addr = buffer.addr();
    }

    @Override
    public long addr() { return addr; }

    public void free() {
      if (buffer != null) {
        buffer.release();
        buffer = null;
        addr = 0;
      }
    }
  }

  public static abstract class PagedWriterImpl implements PagedWriter {

    public abstract void bind(MutablePagedValueVector vector, RowIndexer rowIndexer);

    @Override
    public void setNull(boolean isNull) {
      if (isNull) {
        throw new UnsupportedOperationException("setNull(true)");
      }
    }

    @Override
    public void setInt(int value) {
      throw new UnsupportedOperationException("setInt()");
    }

    @Override
    public void setLong(long value) {
      throw new UnsupportedOperationException("setLong()");
    }

    @Override
    public void setString(String value) {
      throw new UnsupportedOperationException("setString()");
    }
  }

  public static abstract class PagedReaderImpl implements PagedReader {

    public abstract void bind(PagedValueVector vector, RowIndexer rowIndexer);

    @Override
    public boolean isNull() { return false; }

    @Override
    public int getInt() {
      throw new UnsupportedOperationException("getInt()");
    }

    @Override
    public int getLong() {
      throw new UnsupportedOperationException("getLong()");
    }

    @Override
    public String getString() {
      throw new UnsupportedOperationException("getString()");
    }
  }

  public static class ValueIndexer {
    private final RowIndexer rowIndexer;
    private final PagedValueVector vector;
    private final int valueWidth;
    private final int valuesPerPage;
    protected long addr;
    protected int lastRowId;
    protected long endOfPageAddr;

    public ValueIndexer(PagedValueVector vector, RowIndexer rowIndexer, int valueWidth) {
      this.vector = vector;
      this.rowIndexer = rowIndexer;
      this.valueWidth = valueWidth;
      this.valuesPerPage = PAGE_SIZE / valueWidth;
    }

    protected void updateRowId() {
      int rowId = rowIndexer.rowId();
      if (rowId == lastRowId + 1) {
        addr += valueWidth;
        if (addr < endOfPageAddr) {
          lastRowId = rowId;
          return;
        }
      }
      int pageId = rowId / valuesPerPage;
      int offset = (rowId % valuesPerPage) * valueWidth;
      long pageAddr = vector.page(pageId).addr();
      addr = pageAddr + offset;
      endOfPageAddr = pageAddr + PAGE_SIZE;
      lastRowId = rowId;
    }
  }

  public static class IntIndexer extends ValueIndexer {
    public static final int VALUE_WIDTH = 4;

    public IntIndexer(PagedValueVector vector, RowIndexer rowIndexer) {
      super(vector, rowIndexer, VALUE_WIDTH);
    }

    public int get() {
      updateRowId();
      return PlatformDependent.getInt(addr);
    }

    public void set(int value) {
      updateRowId();
      PlatformDependent.putInt(addr, value);
    }

  }

  public static class IntVectorWriter extends PagedWriterImpl {

    private IntIndexer valueIndexer;

    @Override
    public void bind(MutablePagedValueVector vector, RowIndexer rowIndexer) {
      valueIndexer = new IntIndexer(vector, rowIndexer);
    }

    @Override
    public void setInt(final int value) {
      valueIndexer.set(value);
    }

    @Override
    public void copyFrom(PagedReader reader) {
      // TODO Auto-generated method stub

    }
  }

  public interface IntConstants {
    public static final int VALUE_WIDTH = 4;
    public static final int VALUES_PER_PAGE = PAGE_SIZE / VALUE_WIDTH;
  }

  public static class IntVectorWriter2 extends PagedWriterImpl implements IntConstants {

    private RowIndexer rowIndexer;
    private PagedValueVector vector;
    protected long addr;
    protected int lastRowId;
    protected long endOfPageAddr;

    @Override
    public void bind(MutablePagedValueVector vector, RowIndexer rowIndexer) {
      this.vector = vector;
      this.rowIndexer = rowIndexer;
    }

    @Override
    public void setInt(final int value) {
      int rowId = rowIndexer.rowId();
      addr += VALUE_WIDTH;
      if (rowId != lastRowId + 1 || addr >= endOfPageAddr) {
        int pageId = rowId / VALUES_PER_PAGE;
        int offset = (rowId % VALUES_PER_PAGE) * VALUE_WIDTH;
        long pageAddr = vector.page(pageId).addr();
        addr = pageAddr + offset;
        endOfPageAddr = pageAddr + PAGE_SIZE;
      }
      lastRowId = rowId;
      PlatformDependent.putInt(addr, value);
    }

    @Override
    public void copyFrom(PagedReader reader) {
      // TODO Auto-generated method stub

    }
  }

  public static class IntVectorReader extends PagedReaderImpl {

    private IntIndexer valueIndexer;

    @Override
    public void bind(PagedValueVector vector, RowIndexer rowIndexer) {
      valueIndexer = new IntIndexer(vector, rowIndexer);
    }

    @Override
    public int getInt() {
      return valueIndexer.get();
    }
  }

  public static class IntVectorReader2 extends PagedReaderImpl implements IntConstants {

    private RowIndexer rowIndexer;
    private PagedValueVector vector;
    protected long addr;
    protected int lastRowId;
    protected long endOfPageAddr;

    @Override
    public void bind(PagedValueVector vector, RowIndexer rowIndexer) {
      this.vector = vector;
      this.rowIndexer = rowIndexer;
    }

    @Override
    public int getInt() {
      int rowId = rowIndexer.rowId();
      addr += VALUE_WIDTH;
      if (rowId != lastRowId + 1 || addr >= endOfPageAddr) {
        int pageId = rowId / VALUES_PER_PAGE;
        int offset = (rowId % VALUES_PER_PAGE) * VALUE_WIDTH;
        long pageAddr = vector.page(pageId).addr();
        addr = pageAddr + offset;
        endOfPageAddr = pageAddr + PAGE_SIZE;
      }
      lastRowId = rowId;
      return PlatformDependent.getInt(addr);
    }
  }

  public static class PagedVectorAccessorFactory {
    private PagedVectorAccessorFactory() { }

    public static PagedWriterImpl newWriter(MajorType type) {
      // TODO: Create a table

      if (type.getMode() == DataMode.REQUIRED) {
        if (type.getMinorType() == MinorType.INT) {
          return new IntVectorWriter2();
        }
      }
      return null;
    }

    public static PagedReaderImpl newReader(MajorType type) {
      // TODO: Create a table

      if (type.getMode() == DataMode.REQUIRED) {
        if (type.getMinorType() == MinorType.INT) {
          return new IntVectorReader2();
        }
      }
      return null;
    }
  }

  public static long zero = 0;
  public static long foo(long addr) {
    return addr + IntConstants.VALUE_WIDTH + zero;
  }
}
