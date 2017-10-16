package org.apache.drill.exec.physical.impl.scan.project;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.scan.project.RowBatchMerger.Projection;
import org.apache.drill.exec.physical.impl.scan.project.RowBatchMerger.VectorSource;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ColumnProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ScanProjectionParser;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.UnresolvedColumn;
import org.apache.drill.exec.physical.impl.scan.project.TableLevelProjection.ExplicitTableProjection;
import org.apache.drill.exec.physical.impl.scan.project.TableLevelProjection.ResolvedColumn;
import org.apache.drill.exec.physical.impl.scan.project.TableLevelProjection.WildcardTableProjection;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.impl.OptionBuilder;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.physical.rowSet.impl.ResultVectorCacheImpl;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;

public class ScanSchemaOrchestrator {

  public VectorContainer outputContainer;

  public class ReaderSchemaOrchestrator implements VectorSource {

    private ReaderLevelProjection readerProjection;
    private int readerBatchSize;
    private ResultSetLoaderImpl tableLoader;
    private int prevTableSchemaVersion = -1;
    private TableLevelProjection tableProjection;
    private RowBatchMerger batchMerger;
    private VectorContainer tableContainer;

    public ReaderSchemaOrchestrator(ScanSchemaOrchestrator scanOrchestrator) {
      readerProjection = scanOrchestrator.metadataManager.resolve(scanOrchestrator.scanProj);
      readerBatchSize = scanOrchestrator.scanBathSize;
    }

    public void setBatchSize(int size) {
      readerBatchSize = Math.min(size, scanBathSize);
    }

    public ResultSetLoader makeResultSetLoader(TupleMetadata tableSchema) {
      OptionBuilder options = new OptionBuilder();
      options.setRowCountLimit(readerBatchSize);

      // Set up a selection list if available and is a subset of
      // table columns. (Only needed for non-wildcard queries.)
      // The projection list includes all candidate table columns
      // whether or not they exist in the up-front schema. Handles
      // the odd case where the reader claims a fixed schema, but
      // adds a column later.

      if (! scanProj.projectAll()) {
        List<SchemaPath> projectedCols = new ArrayList<>();
        for (ColumnProjection col : readerProjection.output()) {
          if (col.nodeType() == UnresolvedColumn.UNRESOLVED) {
            projectedCols.add(((UnresolvedColumn) col).source());
          }
        }
        options.setProjection(projectedCols);
      }
      options.setSchema(tableSchema);

      // Create the table loader


      tableLoader = new ResultSetLoaderImpl(allocator, options.build());

      // If a schema is given, create a zero-row batch to announce the
      // schema downstream downstream in the form of an empty batch.

      if (tableSchema != null) {
        tableLoader.startBatch();
        endBatch();
      }

      return tableLoader;
    }

//    public ResultSetLoader makeTableLoader(TupleMetadata tableSchema) {
//      if (tableSchema == null) {
//        tableOrchestrator = new LateSchemaOrchestrator(this);
//      } else {
//        tableOrchestrator = new EarlySchemaOrchestrator(this, tableSchema);
//      }
//
//      return tableOrchestrator.makeTableLoader(batchSize);
//    }

    public void endBatch() {
      if (outputContainer != null) {

        // Paranoid: downstream should have consumed the vectors,
        // but clear anyway to avoid a memory leak.

        outputContainer.zeroVectors();
      }
      tableContainer = tableLoader.harvest();
      if (prevTableSchemaVersion < tableLoader.schemaVersion()) {
        reviseOutputProjection();
        prevTableSchemaVersion = tableLoader.schemaVersion();
      }
      int rowCount = tableContainer.getRecordCount();
      metadataManager.load(rowCount);
      if (nullColumnManager != null) {
        nullColumnManager.load(rowCount);
      }
      batchMerger.project(tableContainer.getRecordCount());
    }

    private void reviseOutputProjection() {

      // To refine

      TupleMetadata tableSchema = tableLoader.harvestSchema();
      if (scanProj.hasWildcard()) {
        tableProjection = new WildcardTableProjection(readerProjection,
            tableSchema, this);
      } else {
        tableProjection = new ExplicitTableProjection(readerProjection,
            tableSchema, this,
            nullColumnManager);
      }


//      scanProjector.projectionDefn.startSchema(tableLoader.writer().schema());
//      scanProjector.planProjection();

      List<Projection> vectorProj = new ArrayList<>();
      for (ResolvedColumn col : tableProjection.output) {
        vectorProj.add(col.projection());
      }
      outputContainer = new VectorContainer(allocator);
      batchMerger = new RowBatchMerger(outputContainer, vectorProj);
      batchMerger.buildOutput(vectorCache);
    }

    @Override
    public ValueVector getVector(int fromIndex) {
      return tableContainer.getValueVector(fromIndex).getValueVector();
    }

    @Override
    public BatchSchema getSchema() {
      return tableContainer.getSchema();
    }

    public void close() {
      // TODO Auto-generated method stub

    }
  }

//  public static class EarlySchemaOrchestrator extends TableSchemaOrchestrator {
//
//  }
//
//  public static class LateSchemaOrchestrator {
//
//  }

//  public static class TableSchemaOrchestrator {
//
//    ScanSchemaOrchestrator scanOrchestrator;
//    ReaderSchemaOrchestrator readerOrchestrator;
//    TableLevelProjection tableProjection;
//    VectorSource tableSource;
//    NullColumnManager nullColumnManager;
//
//    public TableSchemaOrchestrator(ReaderSchemaOrchestrator) {
//
//    }
//
//    public void start() {
//      List<TableProjectionResolver> resolvers = new ArrayList<>();
//      if (scanOrchestrator.supportsMetadata) {
//        resolvers.add(scanOrchestrator.metadataManager.resolver());
//      }
//    }
//
//    public void endBatch() {
//
//    }
//  }

  protected List<SchemaPath> projection;
  protected boolean supportsMetadata;
  protected ScanLevelProjection scanProj;
  protected MetadataManager metadataManager;
  protected final ResultVectorCacheImpl vectorCache;
  protected MajorType nullType;
  private ReaderSchemaOrchestrator currentReader;
  private NullColumnManager nullColumnManager;
  private final BufferAllocator allocator;
  protected int scanBathSize = ValueVector.MAX_ROW_COUNT;

  public ScanSchemaOrchestrator(BufferAllocator allocator) {
    this.allocator = allocator;
    vectorCache = new ResultVectorCacheImpl(allocator);
  }

  public void withMetadata(MetadataManager metadataMgr) {
    metadataManager = metadataMgr;
  }

  public void setBatchSize(int batchSize) {
    batchSize = Math.max(1,
        Math.min(batchSize, ValueVector.MAX_ROW_COUNT));
  }

  public void setNullType(MajorType nullType) {
    if (nullColumnManager != null) {
      throw new IllegalStateException("Too late to set the null type.");
    }
    this.nullType = nullType;
  }

  void build(List<SchemaPath> projection) {
    this.projection = projection;

    // Bind metadata manager parser to scan projector

    if (metadataManager == null) {
      metadataManager = new NullMetadataManager();
    }
    List<ScanProjectionParser> parsers = new ArrayList<>();
    ScanProjectionParser parser = metadataManager.projectionParser();
    if (parser != null) {
      supportsMetadata = true;
      parsers.add(parser);
    }
    addParsers(parsers);
    scanProj = new ScanLevelProjection(projection, parsers);
    if (! scanProj.hasWildcard()) {
      nullColumnManager = new NullColumnManager(vectorCache, nullType);
    }
  }

  private void addParsers(List<ScanProjectionParser> parsers) { }

  public ReaderSchemaOrchestrator startReader() {
    closeReader();
    currentReader = new ReaderSchemaOrchestrator(this);
    return currentReader;
  }

  private void closeReader() {
    if (currentReader == null) {
      currentReader.close();
      currentReader = null;
    }
  }

}
