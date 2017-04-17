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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.memory.AbstractMemoryVisitor;
import org.apache.drill.exec.memory.AllocationManager;
import org.apache.drill.exec.memory.BaseAllocator;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.MemoryVisitor;
import org.apache.drill.exec.memory.RootAllocator;

import com.google.common.collect.Sets;

import org.apache.drill.exec.memory.AllocationManager.BufferLedger;

import io.netty.buffer.DrillBuf;
import io.netty.buffer.UnsafeDirectLittleEndian;

public class AllocatorReport {

  public static class Indenter {

    public final StringBuilder buf = new StringBuilder();
    private int indent = 0;
    private final int indentStep;
    private boolean inLine = false;

    public Indenter() {
      indentStep = 2;
    }

    public void indent( ) {
      endLine();
      indent++;
    }

    public void outdent() {
      endLine();
      if (indent > 0) {
        indent--;
      }
    }

    public void endLine() {
      if (inLine) {
        buf.append('\n');
        inLine = false;
      }
    }

    public StringBuilder startLine() {
      endLine();
      final char[] indentation = new char[indent * indentStep];
      Arrays.fill(indentation, ' ');
      buf.append(indentation);
      inLine = true;
      return buf;
    }
  }

  private final Indenter out = new Indenter();
  private boolean showAll = true;
  private boolean showLayout;
  private boolean showDetails;
  private boolean showSummary;

  public class SummaryReport extends AbstractMemoryVisitor {

    @Override
    public void visitAllocator(BaseAllocator alloc) {
      out.startLine()
        .append(alloc.getClass().getSimpleName())
        .append("[")
        .append(alloc.getName())
        .append(", alloc=")
        .append(format(alloc.getAllocatedMemory()))
        .append(", peak=")
        .append(format(alloc.getPeakMemoryAllocation()))
        .append(", limit=")
        .append(format(alloc.getLimit()))
        .append("]");
      out.endLine();
      out.indent();
      alloc.visitChildAllocators(this);
      out.outdent();
    }
  }

  public class LedgerToManagerVisitor extends AbstractMemoryVisitor {
    private Set<AllocationManager> mgrs = Sets.newIdentityHashSet();

    @Override
    public void visitLedger(BufferLedger ledger) {
      mgrs.add(ledger.getManager());
    }
  }

  public class Block {
    final long addr;
    final int size;
    final long id;

    public Block(long addr, int size, long id) {
      this.addr = addr;
      this.size = size;
      this.id = id;
    }

    public Block(long startAddr, long endAddr) {
      this.addr = startAddr;
      size = (int) (endAddr - startAddr);
      id = 0;
    }

    public boolean isFree() { return id == 0; }
  }

  public class DetailReport extends AbstractMemoryVisitor {

    public List<Block> blocks = new ArrayList<>();

    @Override
    public void visitAllocator(BaseAllocator alloc) {
      out.startLine()
        .append(alloc.getClass().getSimpleName())
        .append("[")
        .append(alloc.getName())
        .append(", alloc=")
        .append(format(alloc.getAllocatedMemory()))
        .append("]");
      out.endLine();
      LedgerToManagerVisitor v = new LedgerToManagerVisitor();
      alloc.visitLedgers(v);
      out.indent();
      reportManagers(v.mgrs);
      out.outdent();
      alloc.visitChildAllocators(this);
    }

    private void reportManagers(Set<AllocationManager> mgrs) {
      for (AllocationManager mgr : mgrs) {
        reportManager(mgr);
      }
    }

    private void reportManager(AllocationManager mgr) {
      UnsafeDirectLittleEndian udle = mgr.getUnderlying();
      blocks.add(new Block(udle.memoryAddress(), udle.capacity(), mgr.getId()));
      out.startLine()
        .append(mgr.getClass().getSimpleName())
        .append("[id=")
        .append(mgr.getId())
        .append(", size=")
        .append(format(mgr.getSize()))
        .append("udle[")
          .append("id=")
          .append(udle.getId())
          .append(", addr=")
          .append(hex(udle.memoryAddress()))
          .append(", size=")
          .append(udle.capacity())
          .append("]")
        .append("]");
      out.endLine();
      out.indent();
      mgr.visitLedgers(this);
      out.outdent();
    }

    @Override
    public void visitLedger(BufferLedger ledger) {
      out.startLine()
        .append(ledger.getClass().getSimpleName())
        .append("[id=")
        .append(ledger.getId())
        .append(", refs=")
        .append(ledger.getRefCount())
        .append(", size=")
        .append(format(ledger.getSize()))
        .append("]");
      out.endLine();
    }
  }

  private static String format(long value) {
    return String.format("%,d", value);
  }

  private static String hex(long value) {
    return Long.toHexString(value);
  }

  public AllocatorReport withLayout() {
    showAll = false;
    showLayout = true;
    return this;
  }

  public AllocatorReport withDetails() {
    showAll = false;
    showLayout = true;
    showDetails = true;
    return this;
  }

  public AllocatorReport withSummary() {
    showAll = false;
    showSummary = true;
    return this;
  }

  public void report(BufferAllocator allocator) {
    if (showAll) {
      showSummary = true;
      showDetails = true;
      showLayout = true;
    }
    if ( showSummary) {
      allocator.visit(new SummaryReport());
    }
    if (showDetails || showLayout) {
      DetailReport detail = new DetailReport();
      allocator.visit(detail);
      if (showDetails) {
        System.out.println(out.buf.toString());
      }
      if (showLayout) {
        dumpLayout(detail.blocks);
      }
    }
  }

  private void dumpLayout(List<Block> blocks) {
    List<Block> allBlocks = computeLayout(blocks);
    printSimpleLayout(allBlocks);
  }

  private void printSimpleLayout(List<Block> allBlocks) {
    Indenter out = new Indenter();
    out.startLine()
      .append("Memory Layout");
    out.endLine();
    out.startLine()
      .append("     Address      Size  Alloc");
    out.indent();
    int freeCount = 0;
    int allocCount = 0;
    long freeSize = 0;
    long allocSize = 0;
    for (Block block : allBlocks) {
      if (block.isFree()) {
        freeCount++;
        freeSize += block.size;
      } else {
        allocCount++;
        allocSize += block.size;
      }
      out.startLine()
        .append(String.format("%10x  %8x  %s",
                block.addr, block.size, block.isFree() ? "-----" : Long.toString(block.id)));
      out.endLine();
    }
    out.outdent();
    out.startLine()
    .append("Totals:");
    out.indent();
    out.startLine()
      .append("Alloc: ")
      .append(allocCount)
      .append(" blocks, ")
      .append(hex(allocSize))
      .append(" bytes (hex)");
    out.startLine()
      .append("Free:  ")
      .append(freeCount)
      .append(" blocks, ")
      .append(hex(freeSize))
      .append(" bytes (hex)");
    out.endLine();
    out.startLine()
      .append("Total: ")
      .append(freeCount + allocCount)
      .append(" blocks, ")
      .append(hex(freeSize + allocSize))
      .append(" bytes (hex)");
    out.startLine()
      .append("Max:   ")
      .append(hex(DrillConfig.getMaxDirectMemory()))
      .append(" bytes (hex)");
    out.endLine();
    System.out.println(out.buf.toString());
  }

  private List<Block> computeLayout(List<Block> blocks) {
    Collections.sort(blocks, new Comparator<Block>() {

      @Override
      public int compare(Block o1, Block o2) {
        return Long.compare(o1.addr, o2.addr);
      }

    });

    long posn = 0;
    List<Block> allBlocks = new ArrayList<>();
    for (Block block : blocks) {
      if (posn > 0  &&  posn < block.addr) {
        allBlocks.add(new Block(posn, block.addr));
      }
      allBlocks.add(block);
      posn = block.addr + block.size;
    }
    if ( posn < DrillConfig.getMaxDirectMemory() ) {
      allBlocks.add(new Block(posn, DrillConfig.getMaxDirectMemory()));
    }
    return allBlocks;
  }

}
