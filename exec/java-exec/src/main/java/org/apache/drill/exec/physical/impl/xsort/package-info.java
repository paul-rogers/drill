/**
 * External sort batch (ESB) operator which spills to disk in order 
 * to perform the sort within a defined memory budget.
 * <p>
 * Assume three "generations":
 * <ul>
 * <li>An in-memory generation of sorted batches received from upstream,
 * not yet spilled.</li>
 * <li>A "new" spill generation: those files created directly by spilling
 * the in-memory generation.</li>
 * <li>An "old" spill generation: those files created by re-spilling
 * new generation files.</li>
 * </ul>
 * <p>
 * Spill works as follows:
 * <ul>
 * <li>For each upstream batch, sort it and add it to the in-memory
 * generation.</li>
 * <li>When the in-memory generation reaches the spill threshold, merge the 
 * in-memory batches and write to a spill file. Add the spill file to the 
 * new spill generation. At this point, ESB memory is empty.</li>
 * <li>If the new spill generation has reached the spill threshold, merge 
 * the spilled batches and write to another spill file. Delete the old spill 
 * files. Add the newly created file to the old spill generation. The new 
 * spill generation is now empty (as is memory.)</li>
 * <li>If the old spill generation has reached the spill threshold, transfer 
 * it to the new generation and spill as above. The old generation now has 
 * a single file. (The other two generations are empty.)</li>
 * </ul>
 * <p>
 * The spill threshold is defined as:
 * <ul>
 * <li>Start with the memory budget for the ESB.</li>
 * <li>Define a target spill-batch size. (The minimum of 32K rows or some 
 * defined size in bytes.)</li>
 * <li>Define the maximum number of in-memory batches as 
 * <br><code>memory budget / spill-batch size</code>.</li>
 * <li>Set the spill threshold to some number less than the maximum 
 * in-memory batch size.</li>
 * </ul>
 * When gathering incoming batches in memory, or reading batches from disk, 
 * the above ensures that total memory used is less than the budget.
 * <p>
 * Benefits of this approach:
 * <ul>
 * <li>Minimizes read/writes of existing spilled data (overcomes the 
 * re-spill issue above.)</li>
 * <li>Ensures that disk files are deleted as soon as possible.</li>
 * <li>Ensures that ESB operates within a defined memory budget.</li>
 * <li>Handles data of any size; the algorithm above simply continues to 
 * combine generations as needed. Trades off performance (more disk I/O) 
 * for a fixed memory budget.</li>
 * <li>Limits disk use to no more than twice the amount of spilled data 
 * (to account for merging the old generation).</li>
 * </ul>
 */

package org.apache.drill.exec.physical.impl.xsort;