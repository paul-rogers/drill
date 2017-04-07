package org.apache.drill.exec.physical.impl.xsort;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.test.OperatorFixture;
import org.bouncycastle.util.Arrays;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.DrillBuf;
import sun.misc.Unsafe;

/**
 * Note: only one of these tests can be run at a time. All tests muck with the
 * global direct memory pool, so we cannot run multiple per process.
 */
public class TestMemory {

  public static OperatorFixture fixture;
  private static Unsafe unsafe;

  @BeforeClass
  public static void setup() {
    fixture = OperatorFixture.builder().build();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    fixture.close();
  }

  public static final int NETTY_SLAB = 16 * 1024 * 1024;

  /**
   * Simplest possible test: allocate full Netty blocks to fill
   * up direct memory, then free them all.
   */

  @SuppressWarnings("resource")
  @Test
  public void testAlloc() {
    BufferAllocator allocator = fixture.allocator();
    long startMem = allocator.getAllocatedMemory();
    long maxMem = DrillConfig.getMaxDirectMemory();
    long availMem = maxMem - startMem;
    int maxBlocks = (int) (availMem / NETTY_SLAB);
    List<DrillBuf> bufs = new ArrayList<>();
    for (int i = 0; i < maxBlocks; i++) {
      DrillBuf buf = allocator.buffer(NETTY_SLAB);
      assertEquals(NETTY_SLAB, buf.capacity());
      bufs.add(buf);
    }
    long endMem = allocator.getAllocatedMemory();
    assertTrue(availMem - endMem < NETTY_SLAB);

    freeAll(bufs);
    long finalMem = allocator.getAllocatedMemory();
    assertEquals(startMem, finalMem);
  }

  private void freeAll(List<DrillBuf> bufs) {
    for (int i = 0; i < bufs.size(); i++) {
      bufs.get(i).release();
    }
  }

  @SuppressWarnings("resource")
  @Test
  public void testAllocAlt() {
    BufferAllocator allocator = fixture.allocator();
    long startMem = allocator.getAllocatedMemory();
    long maxMem = DrillConfig.getMaxDirectMemory();
    long availMem = maxMem - startMem;
    int maxBlocks = (int) (availMem / NETTY_SLAB);
    int cycles = maxBlocks / 3;
    int largeBlock = NETTY_SLAB * 2;
    int hugeBlock = NETTY_SLAB * 4;
    DrillBuf buf = allocator.buffer(hugeBlock);
    buf.release();
    List<DrillBuf> bufs = new ArrayList<>();
    for (int i = 0; i < cycles; i++) {
      buf = allocator.buffer(NETTY_SLAB);
      System.out.println(buf.toVerboseString());
      assertEquals(NETTY_SLAB, buf.capacity());
      bufs.add(buf);
      buf = allocator.buffer(largeBlock);
      System.out.println(buf.toVerboseString());
      assertEquals(largeBlock, buf.capacity());
      bufs.add(buf);
    }

    freeAll(bufs);
    long finalMem = allocator.getAllocatedMemory();
    assertEquals(startMem, finalMem);
    DrillBuf buf1 = allocator.buffer(4 * NETTY_SLAB);
    DrillBuf buf2 = allocator.buffer(4 * NETTY_SLAB);
    buf1.release();
    buf2.release();
  }

  /**
   * Demonstration of memory fragmentation. Allocate mixed-size
   * blocks, representing vectors. Free the small ones, keeping
   * the large ones, which simulates a project or selection vector
   * operation. Then, create new, larger vectors representing a
   * subsequent state in the query. Though 1/3 of memory is free,
   * no large blocks are available.
   */
  @SuppressWarnings("resource")
  @Test
  public void testAllocAlt2() {
    BufferAllocator allocator = fixture.allocator();
    long startMem = allocator.getAllocatedMemory();
    
    // How much memory available in this run?
    
    long maxMem = DrillConfig.getMaxDirectMemory();
    long availMem = maxMem - startMem;
    
    // Our base "block" size is 16 MB: the size of the netty slab.
    // Large blocks are twice this size (forcing direct memory
    // allocations) while huge blocks are four-times this size.
    // This is the perfect recipe for fragmentation, if it can
    // occur.
    
    int largeBlock = NETTY_SLAB * 2;
    int hugeBlock = NETTY_SLAB * 4;
    int maxBlocks = (int) (availMem / NETTY_SLAB);
    
    // We will allocate one normal size block and one "large"
    // double-sized block per cycle. How many cycles can we do?
    
    int cycles = maxBlocks / 3;
    
    // Demonstrate that we can allocate huge blocks in general.
    
    DrillBuf buf = allocator.buffer(hugeBlock);
    buf.release();
    
    // Keep track of blocks as we allocate them so we can free
    // them later.
    
    List<DrillBuf> smallBufs = new ArrayList<>();
    List<DrillBuf> largeBufs = new ArrayList<>();
    
    // Fill memory in this pattern:
    // [_][__][_][__]...
    // Small blocks come from Netty, large from Unsafe
    
    for (int i = 0; i < cycles; i++) {
      buf = allocator.buffer(NETTY_SLAB);
      assertEquals(NETTY_SLAB, buf.capacity());
      smallBufs.add(buf);
      buf = allocator.buffer(largeBlock);
      assertEquals(largeBlock, buf.capacity());
      largeBufs.add(buf);
    }

    // Free the small blocks, resulting in:
    //  _ [__] _ [__] _ [__] ...

    freeAll(smallBufs);
    long finalMem = allocator.getAllocatedMemory();
    long stillAlloc = cycles * (long) largeBlock;
    assertEquals(startMem + stillAlloc, finalMem);
    
    // Now, set ourselves up for failure. Allocate a huge block.
    // We know we should be able to do so, we did about 75 (by default)
    // cycles, so we have 75 * NETTY_SLAB memory free.
    
    assertTrue(maxMem - finalMem > cycles * (long) NETTY_SLAB);
    
    // So, we should be able to allocate two blocks of 4 slabs each, right?
    // But, the next line fails because there is no single block available
    // of the desired size.
    
    DrillBuf buf1 = allocator.buffer(4 * NETTY_SLAB);
    DrillBuf buf2 = allocator.buffer(4 * NETTY_SLAB);
    buf1.release();
    buf2.release();
    freeAll(largeBufs);
  }

  public static void main(String args[]) {
    TestMemory obj = new TestMemory();
    try {
      obj.testHeapCacheAllocTime();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @SuppressWarnings("resource")
  @Test
  public void testAllocTime() throws Exception {
    OperatorFixture fixture = OperatorFixture.builder().build();
    BufferAllocator allocator = fixture.allocator();
    List<BufferAllocator> allocs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      allocator = allocator.newChildAllocator(
          "child " + i,
          AbstractBase.DEFAULT_INIT_ALLOCATION,
          AbstractBase.DEFAULT_MAX_ALLOCATION);
      allocs.add( allocator );
    }
    long start = System.currentTimeMillis();
    int allocCount = doAllocRun( allocator, 100 );
    long end = System.currentTimeMillis();
    while (! allocs.isEmpty()) {
      allocs.remove(allocs.size()-1).close();
    }
    System.out.println( "Allocs: " + allocCount);
    System.out.println( "Elapsed ms: " + (end - start));
    fixture.close();
  }

  public void testHeapAllocTime() throws Exception {
    int target = 227;
    long start = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      List<byte[]> bufs = new ArrayList<>();
      for (int j = 0; j < target; j++) {
        bufs.add( new byte[NETTY_SLAB] );
      }
      bufs.clear();
    }
    long end = System.currentTimeMillis();
    System.out.println( "Allocs: " + target * 100);
    System.out.println( "Elapsed ms: " + (end - start));
  }

  public void testHeapCacheAllocTime() throws Exception {
    int target = 256;
    LinkedList<byte[]> pool = new LinkedList<>();
    long start = System.currentTimeMillis();
    for (int j = 0; j < target; j++) {
      pool.add( new byte[NETTY_SLAB] );
    }
    for (int i = 0; i < 100; i++) {
      List<byte[]> bufs = new ArrayList<>();
      for (int j = 0; j < target; j++) {
        byte buf[] = pool.removeFirst();
        Arrays.fill(buf, (byte) 0);
        bufs.add( buf );
      }
      while (! bufs.isEmpty()) {
        pool.addLast(bufs.remove(bufs.size()-1));
      }
      bufs.clear();
    }
    long end = System.currentTimeMillis();
    System.out.println( "Allocs: " + target * 100);
    System.out.println( "Elapsed ms: " + (end - start));
  }

  static {
    try {
      Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      unsafe = (Unsafe) unsafeField.get(null);
    } catch (NoSuchFieldException | SecurityException | IllegalArgumentException
        | IllegalAccessException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private int doAllocRun(BufferAllocator allocator, int count) {
    long startMem = allocator.getAllocatedMemory();
    long maxMem = DrillConfig.getMaxDirectMemory();
    long availMem = maxMem - startMem;
    int maxBlocks = (int) (availMem / NETTY_SLAB);
    for (int i = 0; i < count; i++) {
      List<DrillBuf> bufs = new ArrayList<>();
      for (int l = 0; l < maxBlocks; l++) {
        @SuppressWarnings("resource")
        DrillBuf buf = allocator.buffer(NETTY_SLAB);
        assertEquals(NETTY_SLAB, buf.capacity());
//        buf.fastSetZero(0, NETTY_SLAB);
        unsafe.setMemory(buf.memoryAddress(), NETTY_SLAB, (byte) 0);
        bufs.add(buf);
      }
      freeAll(bufs);
    }
    return count * maxBlocks;
  }
}
