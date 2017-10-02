package org.apache.drill.exec.physical.impl.protocol;

import org.apache.drill.exec.ops.services.OperatorServices;

/**
 * Core protocol for a Drill operator execution.
 *
 * <h4>Lifecycle</h4>
 *
 * <ul>
 * <li>Creation via an operator-specific constructor in the
 * corresponding <tt>RecordBatchCreator</tt>.</li>
 * <li><tt>bind()</tt> called to provide the operator services.</li>
 * <li><tt>buildSchema()</tt> called to define the schema before
 * fetching the first record batch.</li>
 * <li><tt>next()</tt> called repeatedly to prepare each new record
 * batch until EOF or until cancellation.</li>
 * <li><tt>cancel()</tt> called if the operator should quit early.</li>
 * <li><tt>close()</tt> called to release resources. Note that
 * <tt>close()</tt> is called in response to:<ul>
 *   <li>EOF</li>
 *   <li>After <tt>cancel()</tt></li>
 *   <li>After an exception is thrown.</li></ul></li>
 * </ul>
 *
 * <h4>Error Handling</h4>
 *
 * Any method can throw an (unchecked) exception. (Drill does not use
 * checked exceptions.) Preferably, the code will throw a
 * <tt>UserException</tt> that explains the error to the user. If any
 * other kind of exception is thrown, then the enclosing class wraps it
 * in a generic <tt>UserException</tt> that indicates that "something went
 * wrong", which is less than ideal.
 *
 * <h4>Result Set</h4>
 * The operator "publishes" a result set in response to returning
 * <tt>true</tt> from <tt>next()</tt> by populating a
 * {@link BatchAccesor} provided via {@link #batchAccessor()}. For
 * compatibility with other Drill operators, the set of vectors within
 * the batch must be the same from one batch to the next.
 */

public interface OperatorExec {
  public void bind(OperatorServices context);
  BatchAccessor batchAccessor();
  boolean buildSchema();
  boolean next();
  void cancel();
  void close();
}