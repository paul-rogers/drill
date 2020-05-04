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
package org.apache.drill.exec.server.rest;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.util.ValueVectorElementFormatter;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.materialize.QueryDataPackage;
import org.apache.drill.exec.physical.impl.materialize.QueryWritableBatch;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.Acks;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.vector.ValueVector.Accessor;
import org.apache.drill.exec.record.MaterializedField;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Set;

/**
 * {@code WebUserConnectionWrapper} which represents the {@code UserClientConnection} between
 * WebServer and Foreman, for the WebUser submitting the query. It provides
 * access to the {@code UserSession} executing the query. There is no actual physical
 * channel corresponding to this connection wrapper.
 *
 * It returns a close future with no actual underlying
 * {@link io.netty.channel.Channel} associated with it but do have an
 * {@code EventExecutor} out of BitServer EventLoopGroup. Since there is no actual
 * connection established using this class, hence the close event will never be
 * fired by underlying layer and close future is set only when the
 * {@link WebSessionResources} are closed.
 */
public class WebUserConnection extends BaseWebUserConnection {
  private static final Logger logger = LoggerFactory.getLogger(WebUserConnection.class);

  public final List<Map<String, String>> results = Lists.newArrayList();

  public final Set<String> columns = Sets.newLinkedHashSet();

  public final List<String> metadata = new ArrayList<>();

  private int autoLimitRowCount;

  WebUserConnection(WebSessionResources webSessionResources) {
    super(webSessionResources);
  }

  @Override
  public void sendData(RpcOutcomeListener<Ack> listener, QueryDataPackage data) {
    // TODO: Use the underlying batch
    QueryWritableBatch result = data.toWritableBatch();
    // There can be overflow here but DrillBuf doesn't support allocating with
    // bytes in long. Hence we are just preserving the earlier behavior and logging debug log for the case.
    final int dataByteCount = (int) result.getByteCount();

    if (dataByteCount < 0) {
      if (logger.isDebugEnabled()) {
        logger.debug("There is BufferOverflow in dataByteCount: {}",
            dataByteCount);
      }
      listener.success(Acks.OK, null);
      return;
    }

    // Create a ByteBuf with all the data in it.
    final int rows = result.getHeader().getRowCount();
    final BufferAllocator allocator = webSessionResources.getAllocator();
    final DrillBuf bufferWithData = allocator.buffer(dataByteCount);
    try {
      final ByteBuf[] resultDataBuffers = result.getBuffers();

      for (final ByteBuf buffer : resultDataBuffers) {
        bufferWithData.writeBytes(buffer);
        buffer.release();
      }

      final RecordBatchLoader loader = new RecordBatchLoader(allocator);
      try {
        loader.load(result.getHeader().getDef(), bufferWithData);
        // TODO:  Clean:  DRILL-2933:  That load(...) no longer throws
        // SchemaChangeException, so check/clean catch clause below.
        for (int i = 0; i < loader.getSchema().getFieldCount(); ++i) {
          //DRILL-6847:  This section adds query metadata to the REST results
          MaterializedField col = loader.getSchema().getColumn(i);
          columns.add(col.getName());
          StringBuilder dataType = new StringBuilder(col.getType().getMinorType().name());

          //For DECIMAL type
          if (col.getType().hasPrecision()) {
            dataType.append("(");
            dataType.append(col.getType().getPrecision());

            if (col.getType().hasScale()) {
              dataType.append(", ");
              dataType.append(col.getType().getScale());
            }

            dataType.append(")");
          } else if (col.getType().hasWidth()) {
            //Case for VARCHAR columns with specified width
            dataType.append("(");
            dataType.append(col.getType().getWidth());
            dataType.append(")");
          }
          metadata.add(dataType.toString());
        }
        ValueVectorElementFormatter formatter = new ValueVectorElementFormatter(webSessionResources.getSession().getOptions());
        for (int i = 0; i < rows; ++i) {
          final Map<String, String> record = Maps.newHashMap();
          for (VectorWrapper<?> vw : loader) {
            final String field = vw.getValueVector().getMetadata().getNamePart().getName();
            final TypeProtos.MinorType fieldMinorType = vw.getValueVector().getMetadata().getMajorType().getMinorType();
            final Accessor accessor = vw.getValueVector().getAccessor();
            final Object value = i < accessor.getValueCount() ? accessor.getObject(i) : null;
            final String display = value == null ? null : formatter.format(value, fieldMinorType);
            record.put(field, display);
          }
          results.add(record);
        }
      } finally {
        loader.clear();
      }
    } catch (Exception e) {
      boolean verbose = webSessionResources.getSession().getOptions().getBoolean(ExecConstants.ENABLE_VERBOSE_ERRORS_KEY);
      // Wrapping the exception into UserException and then into DrillPBError.
      // It will be thrown as exception in QueryWrapper class.
      // It's verbosity depends on system option "exec.errors.verbose".
      error = UserException.systemError(e).build(logger).getOrCreatePBError(verbose);
    } finally {
      // Notify the listener with ACK.OK both in error/success case because data was send successfully from Drillbit.
      bufferWithData.release();
      listener.success(Acks.OK, null);
    }
  }

  /**
   * For authenticated WebUser no cleanup of {@link WebSessionResources} is done since it's re-used
   * for all the queries until lifetime of the web session.
   */
  public void cleanupSession() { }

  public static class AnonWebUserConnection extends WebUserConnection {

    AnonWebUserConnection(WebSessionResources webSessionResources) {
      super(webSessionResources);
    }

    /**
     * For anonymous WebUser after each query request is completed the {@link WebSessionResources} is cleaned up.
     */
    @Override
    public void cleanupSession() {
      webSessionResources.close();
    }
  }

  /**
   * Sets an autolimit on the size of records to be sent back on the connection
   * @param autoLimitRowCount Max number of records to be sent back to WebServer
   */
  void setAutoLimitRowCount(int autoLimitRowCount) {
    this.autoLimitRowCount = autoLimitRowCount;
  }

  /**
   * Gets the max size of records to be sent back by the query
   * @return Max number of records to be sent back to WebServer
   */
  public int getAutoLimitRowCount() {
    return this.autoLimitRowCount;
  }
}
