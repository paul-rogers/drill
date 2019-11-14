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
package org.apache.drill.exec.store.base;

import java.util.List;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.metastore.MetadataProviderManager;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.server.options.SessionOptionManager;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.base.BaseStoragePlugin.StoragePluginOptions;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Base group scan for storage plugins. A group scan is a "logical" scan:
 * it is the representation of the scan used during the logical and
 * physical planning phases. The group scan is converted to a "sub scan"
 * (an executable scan specification) for inclusion in the physical plan
 * sent to Drillbits for execution. The group scan represents the entire
 * scan of a table or data source. The sub scan divides that scan into
 * files, storage blocks or other forms of parallelism.
 * <p>
 * This class tracks the user name (needed by some external systems),
 * the associated storage plugin and the requested schema (project
 * push-down.) Your subclass should add additional information needed
 * to identify a table and its properties. You can then extend
 * the scan spec if you need to pass along more information, or extend
 * this class to handle extended plan-time behavior such as filter
 * push-down.
 *
 * <h4>Serialization</h4>
 *
 * The group scan is Jackson-serialized to create the logical plan. The
 * logical plan is not normally seen during normal SQL execution, but is
 * used internally for testing. Each subclass must choose which fields
 * to serialize and which to make ephemeral in-memory state. This is
 * tricky. There are multiple cases:
 * <p><ol>
 * <li>Materialize and serialize the data. This class serializes a
 * the user name (base class) and list of columns.</li>
 * <li>Serialized ephemeral data. The base class marks the scan
 * stats as a JSON field, however the scan stats are usually
 * recomputed each time they are needed since they change as the
 * scan is refined. This class will serialize the storage plugin
 * config, and gets that from the storage plugin itself. This class
 * caches scan stats, but does not serialize them.</li>
 * <li>Non-serialized materialized data. A base class of this class
 * includes an id which is not directly serialized. This class
 * holds a reference to the storage plugin.</li>
 * <li>Cached data. Advanced storage plugins work with an external system
 * and will want to cache metadata from that system. Such metadata
 * should be cached on the storage plugin, not in this class.</li>
 * </ol>
 * <p>Jackson will use the constructor marked with
 * <tt>@JsonCreator</tt> to deserialize your group scan. If you
 * create a subclass, and add fields, start with the constructor
 * from this class, then add your custom fields after the fields
 * defined here.
 *
 * <h4>Life Cycle</h4>
 *
 * Drill uses Calcite for planning. Calcite is very complex: it applies
 * a series of rules to transform the query, then chooses the lowest cost
 * among the available transforms. As a result, the group scan object
 * is continually created and recreated. The following is a rough outline
 * of these events.
 *
 * <h5>Create the Group Scan</h5>
 *
 * The storage plugin provides a {@link SchemaFactory}
 * which provides a {@link SchemaConfig} which represents the set of
 * (logical) tables available from the plugin.
 * <p>
 * Calcite offers table
 * names to the schema. If the table is valid, the schema creates a
 * {@link BaseScanSpec} to describe the schema, table and other
 * information unique to the plugin. The schema then creates a group scan
 * using the following constructor:
 * {@link #BaseGroupScan(BaseStoragePlugin, String, BaseScanSpec)}.
 *
 * <h5>Column Resolution</h5>
 *
 * Calcite makes multiple attempts to refine the set of columns
 * from the scan. If we have the following query:<br><code><pre>
 * SELECT a AS x, b FROM myTable</pre></code><br>
 * Then Calcite will offer the following set of columns as planning
 * proceeds:
 *
 * <ol>
 * <li>['**'] (The default starter set.)</li>
 * <li>['**', 'a', 'b', 'x'] (alias not yet resolved.)</li>
 * <li>['a', 'b'] (aliases resolved)</li>
 * </ol>
 *
 * Each time the column set changes, Calcite makes a copy of the group
 * scan by calling {@link AbstractGroupScan#clone(List<SchemaPath>)}.
 * This class automates the process by calling two constructors. First,
 * it calls
 * {@link BaseScanSpec#BaseScanSpec(BaseScanSpec, List<SchemaPath>)}
 * to create a new scan spec with the columns included.
 * <p>
 * The easiest solution is simply to provide the needed constructors.
 * For special cases, you can override the <tt>clone()</tt> method
 * itself.
 *
 * <h5>Intermediate Copies</h5>
 *
 * At multiple points, Calcite will create a simple copy of the
 * node by invoking
 * {@link GroupScan#getNewWithChildren(List<PhysicalOperator>)}.
 * Scans never have children, so this method should just make a
 * copy. This base class automatically invokes the
 * {@link BaseScanFactory#copyGroup()} method.
 * <p>
 * At some point, Drill serializes the scan spec to JSON, then recreates
 * the group scan via one of the
 * {@link AbstractStoragePlugin#getPhysicalScan(String userName, JSONOptions selection,
 *    SessionOptionManager sessionOptions,
 *    MetadataProviderManager metadataProviderManager)}
 * methods. Each offer different levels of detail.
 * The framework handles this case via the
 * {@link BaseScanFactory#newGroupScan()} method.
 *
 * <h5>Node Assignment</h5>
 *
 * Drill calls {@link #getMaxParallelizationWidth()} to determine how
 * much it can parallelize the scan. Then, for each minor fragment,
 * Drill calls {@link #getSpecificScan(int)}. If your scan is a
 * simple single scan, you can set the
 * {@link StoragePluginOptions#maxParallelizationWidth} value to 1
 * and assume that Drill will create a single sub scan. Full
 * distribution is more advanced; see existing plugins for how to
 * handle that in various case.
 *
 * <h4>The Storage Plugin</h4>
 *
 * Group scans are ephemeral and serialized. They should hold only data that
 * describes the scan. Group scans <i>should not</i> hold metadata about
 * the underlying system because of the complexity of recreating that
 * data on each of the many copies that occur.
 * <p>
 * Instead, the implementation should cache metadata in the storage plugin.
 * Each storage plugin instance exists for the duration of a single query:
 * either at plan time or (if requested) at runtime (one instance per
 * minor fragment.) A good practice is:
 * <ul>
 * <li>The group scan asks the storage plugin for metadata as needed
 * to process each group scan operation.</li>
 * <li>The storage plugin retrieves the metadata from the external
 * system and caches it; returning the cached copies on subsequent
 * requests.</li>
 * <li>The sub scan (execution description) should avoid asking for
 * metadata to avoid caching metadata in each of the many execution
 * minor fragments.</li>
 * <li>Cached information is lost once planning completes for a query.
 * If additional caching is needed, the storage plugin can implement a
 * shared cache (with proper concurrency controls) which is shared
 * across multiple plugin instances. (Note, however, than planning
 * is also distributed; each query may be planned on a different
 * Drillbit (Foreman), so even a shared cache will hold as many copies
 * as there are Drillbits (one copy per Drillbit.)</li>
 * </ul>
 * <p>
 * The storage plugin is the only part of the data for a query that
 * persists across the entire planning session. Group scans are
 * created and deleted. Although some of these are done via copies
 * (and so could preserve data), the <tt>getPhysicalScan()</tt> step
 * is not a copy and discards all data except that in the scan spec.
 *
 * <h4>The Scan Specification</h4>
 *
 * Drill has the idea of a scan specification, as mentioned above.
 * This class is unique to each storage plugin.
 * Your subclass should include query-specific data needed at
 * both plan and run time such as file locations, partition information,
 * filter push-downs and so on.
 * <p>
 * Scan specification data is often converted to a different form
 * for the specific needs of each plugin. Your group scan (or even
 * sub scan) can simply hold onto the scan spec, or can replace
 * it with information specfic to the external system. For example,
 * Drill's schema &amp; table system does not really apply to a
 * REST service which will, instead, need an endpoint.
 *
 * <h4>Costs</h4>
 *
 * Calcite is a cost-based optimizer. This means it uses the cost
 * associated with a group scan instance to pick which of several
 * options to use.
 * <p>
 * You must provide a scan cost estimate in terms of rows, data
 * and CPU. For some systems (such as Hive), these numbers are
 * available. For others (such as local files), the data size
 * may be available, from which we can estimate a row count by
 * assuming some reasonable average row width. In other cases
 * (such as REST), we may not have any good estimate at all.
 * <p>
 * In these cases, it helps to know how Drill uses the cost
 * estimates. The scan must occur, so there is no decsion about
 * whether to use the scan or not. But, Drill has to decide which
 * scan to put on which side of a join (the so-called "build" and
 * "probe" sides.) Further, Drill needs to know if the table is
 * small enough to "broadcast" the contents to all nodes.
 * <p>
 * So, at the least, the estimate must identify "small" vs.
 * "large" tables. If the scan is known to be small enough to
 * broadcast (because it is for, say, a small lookup list), then
 * provide a small row and data estimate, say 100 rows and 1K.
 * <p>
 * If, however, the data size is potentially large, then provide
 * a large estimate (10K rows, 100M data, say) to force Drill to
 * not consider broadcast, and to avoid putting the table on the
 * build side of a join unless some other table is even larger.
 *
 * <h5>Projection Push-Down</h5>
 *
 * Suppose your plugin accepts projection push-down. You indicate
 * this by setting
 * {@link StoragePluginOptions#supportsProjectPushDown} to <tt>true</tt>
 * in your storage plugin.
 * <p>
 * Calcite must decide whether to actually do the push down. While
 * this seems like an obvious improvement, Calcite wants the numbers.
 * So, the cost of a scan should be lower (if only by a bit) with
 * project push-down than without. (Also, the cost of a project
 * operator will be removed with push-down, biasing the choice
 * in favor of push-down.
 *
 * <h5>Filter Push-Down</h5>
 *
 * If your plugin supports filter push-down, then the cost with
 * filters applied must be lower than the cost without the filter
 * push-down, or Calcite may not choose to do the push-down. Filter
 * push-down should reduce the number of rows scanned, and that
 * reduction (or even a crude estimate, such as 50%) needs to be
 * reflected in the cost.
 * <p>
 * Your filter push-down competes with Drill's own filter operator.
 * If the filter operator, along with a scan without push down,
 * is less expensive than a scan with push down, then Calcite will
 * choose the former. Thus, the selectivity of filter push down
 * must exceed the selectivity which Drill uses internally.
 * (Drill uses the default selectivity defined by Calcite.)
 *
 * <h4>EXPLAIN PLAN</tt>
 *
 * The group scan appears in the output of the
 * <tt>EXPLAIN PLAN FOR</tt> command. Drill calls the
 * {@ink #toString()} method to obtain the string. The format of the string
 * should follow Drill's conventions. The easiest way to to that is to
 * instead override {@link #buildPlanString(PlanStringBuilder)}
 * and add your custom fields to those already included from this base
 * class.
 * <p>
 * If your fields have structure, ensure that the <tt>toString()</tt>
 * method of those classes also uses {@link PlanStringBuilder}, or
 * create the encoding yourself in your own <tt>buildPlanString</tt>
 * method. Test by calling the method or by examining the output
 * of an <tt>EXPLAIN</tt>.
 */

public abstract class BaseGroupScan extends AbstractGroupScan {

  protected final BaseStoragePlugin<?> storagePlugin;
  protected final List<SchemaPath> columns;
  protected ScanStats scanStats;

  public BaseGroupScan(BaseStoragePlugin<?> storagePlugin,
      String userName, List<SchemaPath> columns) {
    super(userName);
    this.storagePlugin = storagePlugin;
    this.columns = columns;
  }

  public BaseGroupScan(BaseGroupScan from) {
    this(from.storagePlugin, from.getUserName(), from.getColumns());
  }

  @JsonCreator
  public BaseGroupScan(
      @JsonProperty("config") StoragePluginConfig config,
      @JsonProperty("userName") String userName,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JacksonInject StoragePluginRegistry engineRegistry) {
    super(userName);
    this.storagePlugin = BaseStoragePlugin.resolvePlugin(engineRegistry, config);
    this.columns = columns;
  }

  @JsonProperty("config")
  public StoragePluginConfig getConfig() { return storagePlugin.getConfig(); }

  @JsonProperty("columns")
  @Override
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @SuppressWarnings("unchecked")
  public <T extends BaseStoragePlugin<?>> T storagePlugin() {
    return (T) storagePlugin;
  }

  public StoragePluginOptions pluginOptions() {
    return storagePlugin.options();
  }

  @Override
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return pluginOptions().supportsProjectPushDown;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) { }

  @JsonIgnore
  @Override
  public int getMaxParallelizationWidth() {
    return pluginOptions().maxParallelizationWidth;
  }

  @JsonIgnore
  @Override
  public ScanStats getScanStats() {
    if (scanStats == null) {
      scanStats = computeScanStats();
    }
    return scanStats;
  }

  /**
   * Compute the statistics for the scan used to plan the query.
   * In general, the most critical aspect is to give a rough
   * estimate of row count: a small row count will encourage the
   * planner to broadcast rows to all Drillbits, something that is
   * undesirable if the row count is actually large. Otherwise, more
   * accurate estimates are better.
   */

  protected abstract ScanStats computeScanStats();

  /**
   * Create a sub scan for the given minor fragment. Override this
   * if you define a custom sub scan. Uses the default
   * {@link BaseSubScan} by default.
   */

  @Override
  public abstract SubScan getSpecificScan(int minorFragmentId);

  @JsonIgnore
  @Override
  public String getDigest() { return toString(); }

  /**
   * Create a copy of the group scan with with candidate projection
   * columns.
   * Automatically calls the derived class copy + fields constructor by
   * default.
   */

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    return pluginOptions().scanFactory.groupWithColumnsShim(this, columns);
  }

  /**
   * Create a copy of the group scan with (non-existent) children.
   * Automatically calls the derived class copy constructor by
   * default.
   */
  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return pluginOptions().scanFactory.copyGroupShim(this);
  }

  /**
   * Create a string representation of the scan formatted for the
   * <tt>EXPLAIN PLAN FOR</tt> output. Do not override this method,
   * override {@link #buildPlanString(PlanStringBuilder)} instead.
   */

  @Override
  public String toString() {
    PlanStringBuilder builder = new PlanStringBuilder(this);
    buildPlanString(builder);
    return builder.toString();
  }

  /**
   * Build an <tt>EXPLAIN PLAN FOR</tt> string using the builder
   * provided. Call the <tt>super</tt> method first, then add
   * fields for the subclass.
   * @param builder simple builder to create the required
   * format
   */

  public void buildPlanString(PlanStringBuilder builder) {
    builder.field("user", getUserName());
    builder.field("columns", columns);
  }
}
