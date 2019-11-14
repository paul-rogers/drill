/**
 * Provides a set of base classes for creating a storage plugin.
 * Handles the "boilerplate" which is otherwise implemented via
 * copy-and-paste.
 * <p>
 * Drill uses Calcite for planning. The storage plugin tightly
 * integrates with Calcite for a number of tasks:
 * <ul>
 * <li>Create an implementation of {@link BaseStoragePlugin} to
 * orchestrate the process.</li>
 * <li>Create an implementation of {@link BaseStoragePluginConfig}
 * to specify information common to all queries using the plugin.</li>
 * <li>Identify the schema (set of tables) available to the query
 * via an implementation of {@link SchemaFactory}.</li>
 * <li>Resolve tables with the schema to return a plugin-specific,
 * JSON serializable "scan spec".</li>
 * <li>Convert the scan spec to a "group scan" derived from the
 * {@link BaseGroupScan}.</li>
 * <li>Negotiate a set of columns available to the scan. The easiest
 * solution is to use the {@link DrillDynamicTable} that will pretend
 * that every requested column exists.</li>
 * <li>Optionally negotiate filter push down, using a plugin-specific
 * implementation of the {@link FilterPushDownListerner}.</li>
 * <li>Determine how to distribute a query across Drillbits.</li>
 * <li>Create a JSON-serializable implementation of
 * {@link BaseSubScan} that carries per-scan information from the
 * planner to each execution Drillbit.</li>
 * <li>Create a {@link BatchReacher} implementation that uses
 * the {@link ResultSetLoader} and related framework to read data into
 * value vectors.</li>
 * The simplest possible plugin will use many of the base
 * classes as-is, and will implement:
 * <ul>
 * <li>The storage plugin configuration (needed to identify the plugin),</li>
 * <li>The storage plugin class,</li>
 * <li>The schema factory for the plugin (which says which schemas
 * or tables are available),<.li>
 * <li>The batch reader to read the data for the plugin.<.li>
 * </ul>
 *
 * Super classes require a number of standard methods to make
 * copies, present configuration and so on. As much as possible,
 * this class handles those details. For example, the
 * {@link StoragePluginOptions} class holds many of the options
 * that otherwise require one-line method implementations.
 * The framework automatically makes copies of scan objects
 * to avoid other standard methods. Also, the
 * {@link BaseScanFactory} interface handles most of the routine
 * creations and rewrites of group scans.
 * <p>
 * As a plugin gets more complex, it can create its own
 * group and sub scans, add filter push down, and so on.
 * <p>
 * The group scan and sub scan classes should be (mostly) immutable:
 * Calcite will make copies as planning proceeds, then compare the
 * cost of the alternatives. Thus, you must make a copy each time
 * something changes, else Calcite will compare an object with itself
 * and produce non-deterministic results.
 * <p>
 * Also, be sure to compute an estimated cost for your scan, then to
 * reduce that cost each time a push-down (project or filter) occurs.
 * Else, again, Calcite will see no improvement and may go with the
 * original, un-pushed version.
 *
 * @see {@link DummyStoragePlugin} for an example how this
 * framework is used. The Dummy plugin is the "test mule"
 * for this framework.
 */

package org.apache.drill.exec.store.base;
