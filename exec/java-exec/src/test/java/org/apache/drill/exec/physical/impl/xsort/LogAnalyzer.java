/**
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;

public class LogAnalyzer {

  public static class EsbEvent {

    public long elapsed;
    public String thread;

    public EsbEvent( String line ) {
      Pattern p = Pattern.compile( "^(\\d+) \\w+ \\[([^]]+)\\]" );
      Matcher m = match( p, line );
      elapsed = Long.parseLong( m.group( 1 ) );
      thread = m.group( 2 );
    }

    protected Matcher match(Pattern p, String line) {
      Matcher m = p.matcher( line );
      if ( ! m.find() ) {
        throw new IllegalStateException( "Parse error" );
      }
      return m;
    }

    public void updateThread( ThreadTracker thread ) { }
  }

  public static class ExampleEvent extends EsbEvent {

    public ExampleEvent( String line ) {
      super( line );
    }

  }

  public static class ConfigEvent extends EsbEvent {

    public long memoryLimit;
    public int batchLimit;
    public int minSpillLimit;
    public int maxSpillLimit;
    public int mergeLimit;

    public ConfigEvent( String line ) {
      super( line );

      Pattern p = Pattern.compile( "memory limit = (\\d+), batch limit = (\\d+), min, max spill limit: (\\d+), (\\d+), merge limit = (\\d+)" );
      Matcher m = match( p, line );
      memoryLimit = Long.parseLong( m.group(1) );
      batchLimit = Integer.parseInt( m.group(2) );
      minSpillLimit = Integer.parseInt( m.group(3) );
      maxSpillLimit = Integer.parseInt( m.group(4) );
      mergeLimit = Integer.parseInt( m.group(5) );
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      thread.stats.memoryLimit = memoryLimit;
    }
  }

  public static class SortEvent extends EsbEvent {

    public long memory;

    public SortEvent( String line ) {
      super( line );
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      thread.stats.updateMemory( memory );
    }
  }

  public static class SortStartEvent extends SortEvent {
    private int batchCount;
    private int recordCount;

    public SortStartEvent( String line ) {
      super( line );

      Pattern p = Pattern.compile( "Batches = (\\d+), Records = (\\d+), Memory = (\\d+)" );
      Matcher m = match( p, line );
      batchCount = Integer.parseInt( m.group(1) );
      recordCount = Integer.parseInt( m.group(2) );
      memory = Integer.parseInt( m.group(3) );
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      super.updateThread( thread );
      thread.sortStart = elapsed;
      thread.stats.loadBatchCount = batchCount;
      thread.stats.loadRecordCount = recordCount;
    }
  }

  public static class SortEndEvent extends SortEvent {
    public SortEndEvent( String line ) {
      super( line );

      Pattern p = Pattern.compile( "Memory = (\\d+)" );
      Matcher m = match( p, line );
      memory = Long.parseLong( m.group( 1 ) );
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      super.updateThread( thread );
      thread.sortEnd = elapsed;
    }
  }

  public static class MemoryEstimateEvent extends EsbEvent {

    public int recordSize;
    public int targetRecords;
    public int inputBatchSize;
    public int outputBatchSize;
    public long inputMemLimit;
    public long mergeMemLimit;

    public MemoryEstimateEvent( String line ) {
      super( line );

      Pattern p = Pattern.compile( "record: size=(\\d+), target records=(\\d+); batch: input size=(\\d+), output size=(\\d+); memory: input=(\\d+), merge=(\\d+)" );
      Matcher m = match( p, line );
      recordSize = Integer.parseInt( m.group(1) );
      targetRecords = Integer.parseInt( m.group(2) );
      inputBatchSize = Integer.parseInt( m.group(3) );
      outputBatchSize = Integer.parseInt( m.group(4) );
      inputMemLimit = Long.parseLong( m.group(5) );
      mergeMemLimit = Long.parseLong( m.group(6) );
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      thread.stats.updateRecordSize( recordSize );
    }
  }

  public static class LoadStartEvent extends EsbEvent {

    public LoadStartEvent( String line ) {
      super( line );
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      thread.loadStart = elapsed;
    }
  }

  public static class LoadEndEvent extends EsbEvent {

    public LoadEndEvent( String line ) {
      super( line );
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      thread.loadEnd = elapsed;
    }
  }

  public static class DeliveryEndEvent extends EsbEvent {

    private int batchCount;
    private int recordCount;

    public DeliveryEndEvent( String line ) {
      super( line );

      Pattern p = Pattern.compile( "Returned (\\d+) batches, (\\d+) records" );
      Matcher m = match( p, line );
      batchCount = Integer.parseInt( m.group(1) );
      recordCount = Integer.parseInt( m.group(2) );
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      thread.deliveryEnd = elapsed;
      thread.stats.deliveryBatchCount = batchCount;
      thread.stats.deliveryRecordCount = recordCount;
    }
  }

  public static class SpillEvent extends EsbEvent {

    private long memory;
    private int spillCount;
    private int batchCount;

    public SpillEvent( String line ) {
      super( line );

      Pattern p = Pattern.compile( "Memory = (\\d+), Batch count = (\\d+), Spill count = (\\d+)" );
      Matcher m = match( p, line );
      memory = Long.parseLong( m.group(1) );
      batchCount = Integer.parseInt( m.group(2) );
      spillCount = Integer.parseInt( m.group(3) );
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      thread.stats.updateMemory( memory );
      thread.stats.gen1SpillCount++;
      thread.stats.gen1SpillBatchCount += spillCount;
    }
  }

  public static class SpillWriteBatchEvent extends EsbEvent {

    private int recordCount;
    private long timeUs;

    public SpillWriteBatchEvent( String line ) {
      super( line );
      Pattern p = Pattern.compile( "Wrote (\\d+) records in (\\d+)" );
      Matcher m = match( p, line );
      recordCount = Integer.parseInt( m.group(1) );
      timeUs = Long.parseLong( m.group(2) );
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      thread.stats.spillWriteBatchCount++;
      thread.stats.spillWriteRecordCount += recordCount;
      thread.stats.spillWriteTimeUs += timeUs;
    }
  }

  public static class SpillReadBatchEvent extends EsbEvent {

    private int recordCount;
    private long timeUs;

    public SpillReadBatchEvent( String line ) {
      super( line );
      Pattern p = Pattern.compile( "Read (\\d+) records in (\\d+)" );
      Matcher m = match( p, line );
      recordCount = Integer.parseInt( m.group(1) );
      timeUs = Long.parseLong( m.group(2) );
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      thread.stats.spillReadBatchCount++;
      thread.stats.spillReadRecordCount += recordCount;
      thread.stats.spillReadTimeUs += timeUs;
    }
  }

  public static class SpillWriteSummaryEvent extends EsbEvent {

    private long byteCount;
    private String fileName;

    public SpillWriteSummaryEvent( String line ) {
      super( line );

      Pattern p = Pattern.compile( "Wrote (\\d+) bytes to (.+)" );
      Matcher m = match( p, line );
      byteCount = Long.parseLong( m.group(1) );
      fileName = m.group( 2 );
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      thread.stats.spillWriteByteCount += byteCount;
      thread.stats.spillWriteFileCount++;
      thread.fileCount--;
    }
  }

  public static class SpillReadSummaryEvent extends EsbEvent {

    private long byteCount;

    public SpillReadSummaryEvent( String line ) {
      super( line );

      Pattern p = Pattern.compile( "Read (\\d+) bytes" );
      Matcher m = match( p, line );
      byteCount = Long.parseLong( m.group(1) );
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      thread.stats.spillReadByteCount += byteCount;
      thread.stats.spillReadFileCount++;
    }
  }

  public static class ConsolidateEvent extends EsbEvent {

    private int memBatches;
    private int spilledRuns;
    private int batchCount;
    private int recordCount;
    private long memory;

    public ConsolidateEvent( String line ) {
      super( line );

      Pattern p = Pattern.compile( "Batches = (\\d+), Records = (\\d+), Memory = (\\d+), In-memory batches (\\d+), spilled runs (\\d+)" );
      Matcher m = match( p, line );
      batchCount = Integer.parseInt( m.group(1) );
      recordCount = Integer.parseInt( m.group(2) );
      memory = Long.parseLong( m.group(3) );
      memBatches = Integer.parseInt( m.group(4) );
      spilledRuns = Integer.parseInt( m.group(5) );
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      thread.stats.updateMemory( memory );
      thread.stats.loadBatchCount = batchCount;
      thread.stats.loadRecordCount = recordCount;
      thread.stats.consolidateMemBatches = memBatches;
      thread.stats.onsolidateSpilledRuns = spilledRuns;
      thread.consolidateStart = elapsed;
    }
  }

  public static class MergeStartEvent extends EsbEvent {

    private int runs;
    private long memory;

    public MergeStartEvent( String line ) {
      super( line );

      Pattern p = Pattern.compile( "Runs = (\\d+), Memory = (\\d+)" );
      Matcher m = match( p, line );
      runs = Integer.parseInt( m.group(1) );
      memory = Long.parseLong( m.group(2) );
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      thread.stats.updateMemory( memory );
      thread.stats.mergeRuns = runs;
      thread.mergeStart = elapsed;
    }
  }

  public static class CreateFileEvent extends EsbEvent {

    private String fileName;

    public CreateFileEvent( String line ) {
      super( line );

      Pattern p = Pattern.compile( "spilling to (.+)" );
      Matcher m = match( p, line );
      fileName = m.group(1);
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      thread.fileCount++;
    }

  }

  public static class Gen2SpillEvent extends EsbEvent {

    private int batches;
    private long memory;

    public Gen2SpillEvent( String line ) {
      super( line );

      Pattern p = Pattern.compile( "Merging (\\d+) in-memory batches, Memory = (\\d+)" );
      Matcher m = match( p, line );
      batches = Integer.parseInt( m.group(1) );
      memory = Long.parseLong( m.group(2) );
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      thread.stats.updateMemory( memory );
      thread.stats.gen2SpillCount++;
      thread.stats.gen2SpillBatchCount += batches;
    }

  }

  public static class Gen2MergeEvent extends EsbEvent {

    private int runs;
    private long memory;

    public Gen2MergeEvent( String line ) {
      super( line );

      Pattern p = Pattern.compile( "Merging (\\d+) on-disk runs, Memory = (\\d+)" );
      Matcher m = match( p, line );
      runs = Integer.parseInt( m.group(1) );
      memory = Long.parseLong( m.group(2) );
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      thread.stats.updateMemory( memory );
      thread.stats.gen2SpillCount++;
      thread.stats.gen2MergeRunCount += runs;
    }

  }

  public static abstract class LogParser
  {
    public List<EsbEvent> events = new ArrayList<>( );

    public void parse(File logFile) throws IOException {
      try ( BufferedReader reader = new BufferedReader( new FileReader( logFile ) ) )
      {
        String line;
        while ( (line = reader.readLine()) != null ) {
          parseLine( line );
        }
      }
    }

    protected abstract void parseLine(String line);

    protected void add(EsbEvent event) {
      events.add( event );
    }
  }

  public static class ManagedESBLogParser extends LogParser
  {
    @Override
    protected void parseLine(String line) {
      if ( line.contains( "xsort.managed.ExternalSortBatch]") ) {
        parseEsbLine( line );
      }
      else if ( line.contains( "xsort.managed.BatchGroup]") ) {
        parseBatchGroupLine( line );
      }
    }

    private void parseEsbLine(String line) {
      if ( line.contains( "Starting in-memory sort" ) ) {
        add( new SortStartEvent( line ) );
      } else if ( line.contains( "Completed in-memory sort" ) ) {
        add( new SortEndEvent( line ) );
      } else if ( line.contains( "updateMemoryEstimates:" ) ) {
        add( new MemoryEstimateEvent( line ) );
      } else if ( line.contains( "] - Config:" ) ) {
        add( new ConfigEvent( line ) );
      } else if ( line.contains( "Start of load phase" ) ) {
        add( new LoadStartEvent( line ) );
      } else if ( line.contains( "Completed load phase" ) ) {
        add( new LoadEndEvent( line ) );
      } else if ( line.contains( "Deliver phase complete" ) ) {
        add( new DeliveryEndEvent( line ) );
      } else if ( line.contains( "Starting spill from memory" ) ) {
        add( new SpillEvent( line ) );
      } else if ( line.contains( "Starting consolidate phase" ) ) {
        add( new ConsolidateEvent( line ) );
      } else if ( line.contains( "Starting merge phase" ) ) {
        add( new MergeStartEvent( line ) );
      } else if ( line.contains( "Merging and spilling to" ) ) {
        add( new CreateFileEvent( line ) );
      } else if ( line.contains( "on-disk runs, Memory" ) ) {
        add( new Gen2MergeEvent( line ) );
      } else if ( line.contains( "in-memory batches, Memory" ) ) {
        add( new Gen2SpillEvent( line ) );
      } else if ( line.contains( "mergeAndSpill: completed" ) ) {
        ;
      } else {
        System.out.println( line );
      }
    }

    private void parseBatchGroupLine(String line) {
      if ( line.contains( "] - Wrote " ) ) {
        add( new SpillWriteBatchEvent( line ) );
      } else if ( line.contains( "Summary: Wrote " ) ) {
        add( new SpillWriteSummaryEvent( line ) );
      } else if ( line.contains( "] - Read " ) ) {
        add( new SpillReadBatchEvent( line ) );
      } else if ( line.contains( "Summary: Read " ) ) {
        add( new SpillReadSummaryEvent( line ) );
      } else {
        System.out.println( line );
      }
    }
  }

  public static class LegacyMergeEvent extends EsbEvent {

    private long mergeTimeUs;
    private int recordCount;

    public LegacyMergeEvent( String line ) {
      super( line );

      Pattern p = Pattern.compile( "Took (\\d+) us to merge (\\d+) records" );
      Matcher m = match( p, line );
      mergeTimeUs = Long.parseLong( m.group(1) );
      recordCount = Integer.parseInt( m.group(2) );
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      thread.stats.deliveryRecordCount += recordCount;
    }
  }

  public static class LegacyEndDeliverEvent extends EsbEvent {

    public LegacyEndDeliverEvent( String line ) {
      super( line );
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      thread.deliveryEnd = elapsed;
    }
  }

  public static class LegacyStartMergeEvent extends EsbEvent {

    private long memory;
    private int batchCount;

    public LegacyStartMergeEvent( String line ) {
      super( line );

      Pattern p = Pattern.compile( "Starting to merge. (\\d+) batch groups. Current allocated memory: (\\d+)" );
      Matcher m = match( p, line );
      batchCount = Integer.parseInt( m.group(1) );
      memory = Long.parseLong( m.group(2) );
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      thread.stats.updateMemory( memory );
      thread.stats.mergeRuns = batchCount;
      thread.loadEnd = elapsed;
      thread.mergeStart = elapsed;
    }
  }

  public static class LegacyStartSpillEvent extends EsbEvent {

    private int memory;

    public LegacyStartSpillEvent( String line ) {
      super( line );

      Pattern p = Pattern.compile( "mergeAndSpill: starting total size in memory = (\\d+)" );
      Matcher m = match( p, line );
      memory = Integer.parseInt( m.group(1) );
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      thread.stats.updateMemory( memory );
      thread.stats.gen1SpillCount++;
    }
  }

  public static class LegacyEndSpillEvent extends EsbEvent {

    private int memory;

    public LegacyEndSpillEvent( String line ) {
      super( line );

      Pattern p = Pattern.compile( "mergeAndSpill: final total size in memory = (\\d+)" );
      Matcher m = match( p, line );
      memory = Integer.parseInt( m.group(1) );
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      thread.stats.updateMemory( memory );
    }
  }

  public static class LegacyStartEvent extends EsbEvent {

    public LegacyStartEvent( String line ) {
      super( line );
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      thread.loadStart = elapsed;
    }
  }

  public static class LegacySpillWriteBatchEvent extends EsbEvent {

    private int recordCount;
    private long timeUs;

    public LegacySpillWriteBatchEvent( String line ) {
      super( line );

      Pattern p = Pattern.compile( "Took (\\d+) us to spill (\\d+) records" );
      Matcher m = match( p, line );
      timeUs = Long.parseLong( m.group(1) );
      recordCount = Integer.parseInt( m.group(2) );
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      thread.stats.spillWriteBatchCount++;
      thread.stats.spillWriteRecordCount += recordCount;
      thread.stats.spillWriteTimeUs += timeUs;
    }
  }

  public static class LegacySpillReadBatchEvent extends EsbEvent {

    private int recordCount;
    private long timeUs;

    public LegacySpillReadBatchEvent( String line ) {
      super( line );

      Pattern p = Pattern.compile( "Took (\\d+) us to read (\\d+) records" );
      Matcher m = match( p, line );
      timeUs = Long.parseLong( m.group(1) );
      recordCount = Integer.parseInt( m.group(2) );
     }

    @Override
    public void updateThread( ThreadTracker thread ) {
      thread.stats.spillReadBatchCount++;
      thread.stats.spillReadRecordCount += recordCount;
      thread.stats.spillReadTimeUs += timeUs;
    }
  }

  public static class LegacyRecordSizeEvent extends EsbEvent {

    private int recordSize;
    private int targetCount;

    public LegacyRecordSizeEvent( String line ) {
      super( line );

      Pattern p = Pattern.compile( "estimated record size = (\\d+), target record count = (\\d+)" );
      Matcher m = match( p, line );
      recordSize = Integer.parseInt( m.group(2) );
      targetCount = Integer.parseInt( m.group(2) );
    }

   @Override
   public void updateThread( ThreadTracker thread ) {
     thread.stats.largestRecord = recordSize;
     thread.stats.smallestRecord = recordSize;
    }

  }

  public static class LegacyFileSummaryEvent extends EsbEvent {

    public LegacyFileSummaryEvent( String line ) {
      super( line );
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      thread.stats.spillWriteFileCount++;
    }
  }

  public static class LegacyGen2SpillEvent extends EsbEvent {

    public LegacyGen2SpillEvent( String line ) {
      super( line );
    }

    @Override
    public void updateThread( ThreadTracker thread ) {
      thread.stats.gen2SpillCount++;
    }
  }

  public static class LegacyESBLogParser extends LogParser
  {
    @Override
    protected void parseLine(String line) {
      if ( line.contains( "xsort.ExternalSortBatch]") ) {
        parseEsbLine( line );
      }
      else if ( line.contains( "xsort.BatchGroup]") ) {
        parseBatchGroupLine( line );
      }
    }

    private void parseEsbLine(String line) {
      if ( line.contains( "us to merge" ) ) {
        add( new LegacyMergeEvent( line ) );
      } else if ( line.contains( "copier returned 0 records" ) ) {
        add( new LegacyEndDeliverEvent( line ) );
      } else if ( line.contains( "Starting to merge." ) ) {
        add( new LegacyStartMergeEvent( line ) );
      } else if ( line.contains( "mergeAndSpill: starting" ) ) {
        add( new LegacyStartSpillEvent( line ) );
      } else if ( line.contains( "mergeAndSpill: estimated record" ) ) {
        add( new LegacyRecordSizeEvent( line ) );
      } else if ( line.contains( "mergeAndSpill: final" ) ) {
        add( new LegacyEndSpillEvent( line ) );
      } else if ( line.contains( "Starting load phase" ) ) {
        add( new LegacyStartEvent( line ) );
      } else if ( line.contains( "Completed spilling to" ) ) {
        add( new LegacyFileSummaryEvent( line ) );
      } else if ( line.contains( "Merging spills" ) ) {
        add( new LegacyGen2SpillEvent( line ) );
      } else {
        System.out.println( line );
      }
    }

    private void parseBatchGroupLine(String line) {
      if ( line.contains( "us to spill" ) ) {
        add( new LegacySpillWriteBatchEvent( line ) );
      } else if ( line.contains( "us to read" ) ) {
        add( new LegacySpillReadBatchEvent( line ) );
      } else {
        System.out.println( line );
      }
    }
  }

  public static class SortStats {
    public int smallestRecord;
    public int largestRecord;

    public long loadTime;
    public int loadPercent;
    public int loadBatchCount;
    public int loadRecordCount;
    public int loadBatchesPerSec;
    public int loadRecordsPerSec;

    public long deliveryTime;
    public int deliverPercent;
    public int deliveryBatchCount;
    public int deliveryRecordCount;
    public int deliveryBatchesPerSec;
    public int deliveryRecordsPerSec;

    public long sortTime;
    public int sortPercent;
    public int sortBatchesPerSec;
    public int sortRecordsPerSec;

    public long totalTime;
    public long memoryLimit;
    public long maxMemory;
    public int memoryPercent;

    public int gen1SpillBatchCount;
    public int gen1SpillCount;
    public int gen2SpillCount;
    public int gen2SpillBatchCount;
    public int gen2MergeRunCount;

    public long spillWriteByteCount;
    public long spillWriteTimeUs;
    public int spillWriteRecordCount;
    public int spillWriteBatchCount;
    public int spillWriteFileCount;
    public long writeTime;
    public int writePercent;

    public long consolidateTime;
    public int onsolidateSpilledRuns;
    public int consolidateMemBatches;
    public int consolidatePercent;

    public int spillReadFileCount;
    public int spillReadBatchCount;
    public long spillReadByteCount;
    public long spillReadTimeUs;
    public int spillReadRecordCount;
    public long readTime;
    public int readPercent;

    public int mergeRuns;

    public void updateRecordSize( int size ) {
      updateRecordSizes( size, size );
    }

    public void updateRecordSizes( int smallest, int largest ) {
      if ( smallestRecord == 0 ) {
        smallestRecord = smallest;
        largestRecord = largest;
      } else {
        smallestRecord = Math.min( smallestRecord, smallest );
        largestRecord = Math.max( largestRecord, largest );
      }
    }

    public void updateMemory( long memoryUse ) {
      maxMemory = Math.max( maxMemory, memoryUse );
    }

    public void analyze( ) {
      writeTime = Math.round( spillWriteTimeUs / 1000.0 );
      readTime = Math.round( spillReadTimeUs / 1000.0 );
      // Not entirely accurate, but most writes occur during load,
      // most reads during delivery (merge).
      loadTime -= writeTime;
      deliveryTime -= readTime;
      totalTime = loadTime + sortTime + deliveryTime + writeTime + consolidateTime + readTime;

      if ( totalTime > 0 ) {
        loadPercent = Math.round( 100 * loadTime / totalTime );
        sortPercent = Math.round( 100 * sortTime / totalTime );
        deliverPercent = Math.round( 100 * deliveryTime / totalTime );
        writePercent = Math.round( 100 * writeTime / totalTime );
        consolidatePercent = Math.round( 100 * consolidateTime / totalTime );
        readPercent = Math.round( 100 * readTime / totalTime );
      }
      if ( memoryLimit > 0 ) {
        memoryPercent = (int) Math.round( 100 * maxMemory / memoryLimit );
      }
      if ( loadTime > 0 ) {
        loadBatchesPerSec = (int) Math.round( loadBatchCount * 1000.0 / loadTime );
        loadRecordsPerSec = (int) Math.round( loadRecordCount * 1000.0 / loadTime );
      }
      if ( sortTime > 0 ) {
        sortBatchesPerSec = (int) Math.round( loadBatchCount * 1000.0 / sortTime );
        sortRecordsPerSec = (int) Math.round( loadRecordCount * 1000.0 / sortTime );
      }
      if ( deliveryTime > 0 ) {
        deliveryBatchesPerSec = (int) Math.round( deliveryBatchCount * 1000.0 / deliveryTime );
        deliveryRecordsPerSec = (int) Math.round( deliveryRecordCount * 1000.0 / deliveryTime );
      }
    }

    public void add( SortStats stats ) {
      updateRecordSizes( stats.smallestRecord, stats.largestRecord );

      loadTime += stats.loadTime;
      sortTime += stats.sortTime;
      deliveryTime += stats.deliveryTime;

      loadBatchCount += stats.loadBatchCount;
      loadRecordCount += stats.loadRecordCount;
      deliveryBatchCount += stats.deliveryBatchCount;
      deliveryRecordCount += stats.deliveryRecordCount;
      memoryLimit += stats.memoryLimit;
      maxMemory += stats.maxMemory;

      gen1SpillBatchCount += stats.gen1SpillBatchCount;
      gen1SpillCount += stats.gen1SpillCount;
      gen2SpillCount += stats.gen2SpillCount;
      gen2MergeRunCount += stats.gen2MergeRunCount;
      gen2SpillBatchCount += stats.gen2SpillBatchCount;

      spillWriteByteCount += stats.spillWriteByteCount;
      spillWriteTimeUs += stats.spillWriteTimeUs;
      spillWriteRecordCount += stats.spillWriteRecordCount;
      spillWriteFileCount += stats.spillWriteFileCount;
      spillWriteBatchCount += stats.spillWriteBatchCount;

      consolidateTime += stats.consolidateTime;
      onsolidateSpilledRuns += stats.onsolidateSpilledRuns;
      consolidateMemBatches += stats.consolidateMemBatches;

      spillReadByteCount += stats.spillReadByteCount;
      spillReadFileCount += stats.spillReadFileCount;
      spillReadBatchCount += stats.spillReadBatchCount;
      spillReadTimeUs += stats.spillReadTimeUs;
      spillReadRecordCount += stats.spillReadRecordCount;

      mergeRuns += stats.mergeRuns;
    }
  }

  public static class ThreadTracker {
    public int fileCount;
    public final String name;
    public final SortStats stats = new SortStats( );
    public long loadEnd;
    public long loadStart;
    public long deliveryEnd;
    public long sortStart;
    public long sortEnd;
    public long consolidateStart;
    public long mergeStart;

    public ThreadTracker(String name) {
      this.name = name;
    }

    public void analyze( ) {
      stats.loadTime = loadEnd - loadStart;
      stats.sortTime = sortEnd - sortStart;
      if ( sortEnd > 0 ) {
        stats.deliveryTime = deliveryEnd - sortEnd;
      } else if ( consolidateStart > 0 ) {
        stats.consolidateTime = mergeStart - consolidateStart;
        stats.deliveryTime = deliveryEnd - mergeStart;
      }
      stats.analyze();
    }
  }

  public static class EventAnalyzer {

    public final SortStats stats = new SortStats( );
    public int threadCount;
    public long timePerThread;

    Map<String,ThreadTracker> threads = new HashMap<>( );

    public void analyze( List<EsbEvent> events ) {
      for ( EsbEvent event : events ) {
        ThreadTracker thread = threads.get( event.thread );
        if ( thread == null ) {
          thread = new ThreadTracker( event.thread );
          threads.put( event.thread, thread );
        }
        event.updateThread( thread );
      }
      for ( ThreadTracker thread : threads.values() ) {
        thread.analyze();
        stats.add( thread.stats );
      }
      stats.analyze();
      threadCount = threads.size();
      if ( threadCount > 0 ) {
        timePerThread = stats.totalTime / threadCount;
      }
    }

    public void report( ) {
      PrintWriter writer = new PrintWriter( new OutputStreamWriter( System.out ) );
      report( writer );
      writer.flush( );
    }

    private void report(PrintWriter writer) {
      writer.println( "Threads:" );
      for ( String key : threads.keySet() ) {
        ThreadTracker thread = threads.get( key );
        threadReport( writer, thread );
      }
      writer.println( "Totals:" );
      writer.print( "  Threads: " );
      writer.println( threadCount );
      writer.print( "  Time per thread: " );
      writer.println( timePerThread );
       statsReport( writer, "  ", stats );
    }

    private void threadReport(PrintWriter writer, ThreadTracker thread) {
      writer.print( "  " );
      writer.println( thread.name );
      statsReport( writer, "    ", thread.stats );
      writer.print( "    Undeleted files: " );
      writer.println( thread.fileCount );
    }

    private void statsReport(PrintWriter writer, String prefix, SortStats stats ) {
      writer.print( prefix );
      writer.print( "Min Record Size: " );
      writer.println( stats.smallestRecord );
      writer.print( prefix );
      writer.print( "Max Record Size: " );
      writer.println( stats.largestRecord );
      writer.print( prefix );
      writer.print( "Total time: " );
      writer.println( stats.totalTime );

      writer.print( prefix );
      writer.println( String.format( "Memory use: %d (%d%%)", stats.maxMemory, stats.memoryPercent ) );
      writer.print( prefix );
      writer.println( "Load:" );
      writer.print( prefix );
      writer.println( String.format( "  Time: %d (%d%%)", stats.loadTime, stats.loadPercent ) );
      writer.print( prefix );
      writer.println( String.format( "  Batches: %d (%d/sec)", stats.loadBatchCount, stats.loadBatchesPerSec ) );
      writer.print( prefix );
      writer.println( String.format( "  Records: %d (%d/sec)", stats.loadRecordCount, stats.loadRecordsPerSec ) );

      if ( stats.sortTime != 0 ) {
        writer.print( prefix );
        writer.println( "Sort:" );
        writer.print( prefix );
        writer.println( String.format( "  Time: %d (%d%%)", stats.sortTime, stats.sortPercent ) );
        writer.print( prefix );
        writer.println( String.format( "  Batches: %d (%d/sec)", stats.loadBatchCount, stats.sortBatchesPerSec ) );
        writer.print( prefix );
        writer.println( String.format( "  Records: %d (%d/sec)", stats.loadRecordCount, stats.sortRecordsPerSec ) );
      } else {
        writer.print( prefix );
        writer.println( "Spill Gen 1:" );
        writer.print( prefix );
        writer.print( "  Spills: " );
        writer.println( stats.gen1SpillCount );
        writer.print( prefix );
        writer.print( "  Spill Batches: " );
        writer.println( stats.gen1SpillBatchCount );
        writer.print( prefix );
        writer.println( "Spill Gen 3:" );
        writer.print( prefix );
        writer.print( "  Spills: " );
        writer.println( stats.gen2SpillCount );
        writer.print( prefix );
        writer.print( "  Runs: " );
        writer.println( stats.gen2MergeRunCount );
        writer.print( prefix );
        writer.print( "  Batches: " );
        writer.println( stats.gen2SpillBatchCount );
        writer.print( prefix );
        writer.println( "Spill Write:" );
        writer.print( prefix );
        writer.print( "  Files: " );
        writer.println( stats.spillWriteFileCount );
        writer.print( prefix );
        writer.print( "  Batches: " );
        writer.println( stats.spillWriteBatchCount );
        writer.print( prefix );
        writer.print( "  Records: " );
        writer.println( stats.spillWriteRecordCount );
        writer.print( prefix );
        writer.print( "  Bytes: " );
        writer.println( stats.spillWriteByteCount );
        writer.print( prefix );
        writer.println( String.format( "  Time: %d (%d%%)", stats.writeTime, stats.writePercent ) );
        writer.print( prefix );
        writer.println( "Merge Read:" );
        writer.print( prefix );
        writer.print( "  Files: " );
        writer.println( stats.spillReadFileCount );
        writer.print( prefix );
        writer.print( "  Batches: " );
        writer.println( stats.spillReadBatchCount );
        writer.print( prefix );
        writer.print( "  Records: " );
        writer.println( stats.spillReadRecordCount );
        writer.print( prefix );
        writer.print( "  Bytes: " );
        writer.println( stats.spillReadByteCount );
        writer.print( prefix );
        writer.println( String.format( "  Time: %d (%d%%)", stats.readTime, stats.readPercent ) );

        writer.print( prefix );
        writer.println( "Consolidate:" );
        writer.print( prefix );
        writer.println( String.format( "  Time: %d (%d%%)", stats.consolidateTime, stats.consolidatePercent ) );
        writer.print( prefix );
        writer.println( String.format( "  Batches: %d", stats.consolidateMemBatches ) );
        writer.print( prefix );
        writer.println( String.format( "  Runs: %d", stats.onsolidateSpilledRuns ) );
        writer.print( prefix );

        writer.println( "Merge:" );
        writer.print( prefix );
        writer.print( "  Merge Runs: " );
        writer.println( stats.mergeRuns );
      }

      writer.print( prefix );
      writer.println( "Delivery:" );
      writer.print( prefix );
      writer.println( String.format( "  Time: %d (%d%%)", stats.deliveryTime, stats.deliverPercent ) );
      writer.print( prefix );
      writer.println( String.format( "  Batches: %d (%d/sec)", stats.deliveryBatchCount, stats.deliveryBatchesPerSec ) );
      writer.print( prefix );
      writer.println( String.format( "  Records: %d (%d/sec)", stats.deliveryRecordCount, stats.deliveryRecordsPerSec ) );
    }
  }

  boolean useManaged;
  File logFile;
  FileAppender<ILoggingEvent> fileAppender;

  public LogAnalyzer( boolean flag ) {
    useManaged = flag;
  }

  public void setupLogging( ) {
    LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
    Logger sortLogger;
    Logger batchGroupLogger;
    if ( useManaged ) {
      sortLogger = (Logger) LoggerFactory.getLogger(org.apache.drill.exec.physical.impl.xsort.managed.ExternalSortBatch.class);
      batchGroupLogger = (Logger) LoggerFactory.getLogger(org.apache.drill.exec.physical.impl.xsort.managed.BatchGroup.class);
    } else {
      sortLogger = (Logger) LoggerFactory.getLogger(org.apache.drill.exec.physical.impl.xsort.ExternalSortBatch.class);
      batchGroupLogger = (Logger) LoggerFactory.getLogger(org.apache.drill.exec.physical.impl.xsort.BatchGroup.class);
    }
    sortLogger.setLevel(Level.TRACE);
    batchGroupLogger.setLevel(Level.TRACE);

    PatternLayoutEncoder ple = new PatternLayoutEncoder();
//    ple.setPattern("%level [%thread] [%file:%line] %msg%n");
    ple.setPattern("%r %level [%thread] [%logger] - %msg%n");
    ple.setContext(lc);
    ple.start();

    try {
      logFile = File.createTempFile( "drill-", ".log" );
      System.out.print( "Log File: " );
      System.out.println( logFile.getAbsolutePath() );
    } catch (IOException e) {
      throw new IllegalStateException( e );
    }

    fileAppender = new FileAppender<>( );
    fileAppender.setContext(lc);
    fileAppender.setFile( logFile.getAbsolutePath() );
    fileAppender.setName("Trace Log");
    fileAppender.setEncoder( ple );
    fileAppender.start();
    sortLogger.addAppender(fileAppender);
    batchGroupLogger.addAppender(fileAppender);

//    if ( toConsole ) {
//      ConsoleAppender<ILoggingEvent> appender = new ConsoleAppender<>( );
//      appender.setContext(lc);
//      appender.setName("Console");
//      appender.setEncoder( ple );
//      appender.start();
//      sortLogger.addAppender(appender);
//      batchGroupLogger.addAppender(appender);
//    }
  }

  public void analyzeLog( ) {
    LogParser parser = parseLog( logFile );
    EventAnalyzer analyzer = new EventAnalyzer( );
    analyzer.analyze( parser.events );
    analyzer.report( );
  }

  private LogParser parseLog(File logFile) {
    try {
      fileAppender.getOutputStream( ).flush( );
    } catch (IOException e) {
      e.printStackTrace();
      throw new IllegalStateException( e );
    }
    LogParser parser;
    if ( useManaged ) {
      parser = new ManagedESBLogParser( );
    } else {
      parser = new LegacyESBLogParser( );
    }
    try  {
      parser.parse( logFile );
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return parser;
  }

}
