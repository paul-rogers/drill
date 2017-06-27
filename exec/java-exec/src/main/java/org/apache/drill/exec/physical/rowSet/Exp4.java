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
    Object writer();
    Object reader();
    int pageCount();
    Page page(int pageId);
    long pageAddr(int pageId);
    void free();
  }

  public interface PagedReaderEx {

  }
  public interface PagedReader {
    boolean isNull(int rowId);
    int getInt(int rowId);
    int getLong(int rowId);
    String getString(int rowId);
  }

  public interface PagedWriter {
    void setNull(int rowId, boolean isNull);
    void setInt(int rowId, int value);
    void setLong(int rowId, long value);
    void setString(int rowId, String value);
    void copyFrom(int rowId, PagedReader reader);
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
    public Object reader() {
      return PagedVectorAccessorFactory.newReader(schema.getType(), this);
    }
  }

  public static class MutablePagedValueVector extends AbstractPagedValueVector {
    private final List<Page> pages = new ArrayList<>();
    private int cachedPageId = -1;
    private long cachedPageAddr = 0;

    public MutablePagedValueVector(MaterializedField schema, PageAllocator allocator) {
      super(schema, allocator);
    }

    @Override
    public int pageCount() { return pages.size(); }

    @Override
    public final Page page(final int pageIndex) {
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
    public long pageAddr(final int pageId) {
      if (pageId == cachedPageId) {
        return cachedPageAddr;
      }
      cachedPageAddr = page(pageId).addr();
      return cachedPageAddr;
    }
//    private final long pageAddrForRow(final int rowId) {
//      final int pageId = rowId >> 0x12;
//      if (pageId != cachedPageId) {
//          cachedPageAddr = page(pageId).addr();
//      }
//      return cachedPageAddr;
//    }

    public final void setInt(final int rowId, final int value) {
      final int pageId = rowId >> 0x12;
      if (pageId != cachedPageId) {
          cachedPageAddr = page(pageId).addr();
      }
      PlatformDependent.putInt(cachedPageAddr + ((rowId & 0x3FFFF) << 2), value);
    }

    @Override
    public Object writer() {
      return PagedVectorAccessorFactory.newWriter(schema.getType(), this);
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
    private final PageImpl pages[];

    public ImmutablePagedValueVector(MutablePagedValueVector source) {
      super(source.schema, source.allocator);
      pages = new PageImpl[source.pages.size()];
      source.pages.toArray(pages);
    }

    @Override
    public int pageCount() { return pages.length; }

    @Override
    public Page page(int pageIndex) { return pages[pageIndex]; }

    @Override
    public long pageAddr(int pageId) {
      return pages[pageId].addr;
    }

    @Override
    public PagedWriter writer() {
      throw new IllegalStateException("Cannot write to an immutable vector.");
    }

    public final int getInt(final int rowId) {
      final long pageAddr = pages[rowId >> 0x12].addr;
      return PlatformDependent.getInt(pageAddr + ((rowId & 0x3FFFF) << 2));
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

    public abstract void bind(MutablePagedValueVector vector);

    @Override
    public void setNull(int rowId, boolean isNull) {
      if (isNull) {
        throw new UnsupportedOperationException("setNull(true)");
      }
    }

    @Override
    public void setInt(int rowId, int value) {
      throw new UnsupportedOperationException("setInt()");
    }

    @Override
    public void setLong(int rowId, long value) {
      throw new UnsupportedOperationException("setLong()");
    }

    @Override
    public void setString(int rowId, String value) {
      throw new UnsupportedOperationException("setString()");
    }
  }

  public static abstract class PagedReaderImpl implements PagedReader {

    public abstract void bind(PagedValueVector vector);

    @Override
    public boolean isNull(int rowId) { return false; }

    @Override
    public int getInt(int rowId) {
      throw new UnsupportedOperationException("getInt()");
    }

    @Override
    public int getLong(int rowId) {
      throw new UnsupportedOperationException("getLong()");
    }

    @Override
    public String getString(int rowId) {
      throw new UnsupportedOperationException("getString()");
    }
  }


  public interface IntConstants {
    public static final int VALUE_WIDTH = 4;
    public static final int VALUES_PER_PAGE = PAGE_SIZE / VALUE_WIDTH;
  }

  public static class IntVectorWriter {

    private final PagedValueVector vector;
    private int cachedPageId = -1;
    private long cachedPageAddr = 0;

    public IntVectorWriter(MutablePagedValueVector vector) {
      this.vector = vector;
    }

//    @Override
//    public void bind(MutablePagedValueVector vector) {
//      this.vector = vector;
//    }

//    @Override
    public void setInt(final int rowId, final int value) {
      final int pageId = rowId >> 0x12;
      if (pageId != cachedPageId) {
        cachedPageAddr = vector.pageAddr(pageId);
      }
      PlatformDependent.putInt(cachedPageAddr + ((rowId & 0x3FFFF) << 2), value);
    }

//    @Override
//    public void copyFrom(int rowId, PagedReader reader) {
//      // TODO Auto-generated method stub
//
//    }
  }

  public static class IntVectorReader {

    private final PagedValueVector vector;

    public IntVectorReader(PagedValueVector vector) {
      this.vector = vector;
    }

    public int getInt(final int rowId) {
      final long pageAddr = vector.pageAddr(rowId >> 0x12);
      return PlatformDependent.getInt(pageAddr + ((rowId & 0x3FFFF) << 2));
    }
  }

  public static class Desperation {

    public static void setInt(final MutablePagedValueVector vector, final int rowId, final int value) {
      final int pageId = rowId >> 0x12;
      final long pageAddr = vector.pageAddr(pageId);
      PlatformDependent.putInt(pageAddr + ((rowId & 0x3FFFF) << 2), value);
    }

    public static int getInt(final PagedValueVector vector, final int rowId) {
      final long pageAddr = vector.pageAddr(rowId >> 0x12);
      return PlatformDependent.getInt(pageAddr + ((rowId & 0x3FFFF) << 2));
    }
  }

  public static class PagedVectorAccessorFactory {
    private PagedVectorAccessorFactory() { }

    public static Object newWriter(MajorType type, MutablePagedValueVector vector) {
      // TODO: Create a table

      if (type.getMode() == DataMode.REQUIRED) {
        if (type.getMinorType() == MinorType.INT) {
          return new IntVectorWriter(vector);
        }
      }
      return null;
    }

    public static Object newReader(MajorType type, PagedValueVector vector) {
      // TODO: Create a table

      if (type.getMode() == DataMode.REQUIRED) {
        if (type.getMinorType() == MinorType.INT) {
          return new IntVectorReader(vector);
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
