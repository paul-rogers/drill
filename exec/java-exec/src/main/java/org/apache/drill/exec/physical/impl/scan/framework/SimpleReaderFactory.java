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
package org.apache.drill.exec.physical.impl.scan.framework;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework.ReaderFactory;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework.ScanFrameworkBuilder;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiatorImpl.NegotiatorListener;

public abstract class SimpleReaderFactory implements ReaderFactory {
  private ManagedScanFramework framework;
  private ScanFrameworkBuilder options;

  @Override
  public void bind(ManagedScanFramework framework) {
    this.framework = framework;
    options = framework.builder;
    assert framework.builder.readerClass() != null;
    assert framework.builder.negotiatorClass() != null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean open(ManagedReader<? extends SchemaNegotiator> reader,
      NegotiatorListener listener) {
    SchemaNegotiatorImpl schemaNegotiator = new SchemaNegotiatorImpl();
    schemaNegotiator.bind(framework, listener);
    return ((ManagedReader<SchemaNegotiator>) reader).open(schemaNegotiator);
  }

  protected ManagedReader<? extends SchemaNegotiator> newReader() {
    try {
      return options.readerClass().newInstance();
    } catch (ReflectiveOperationException e) {
      throw UserException.systemError(e)
        .build(ManagedScanFramework.logger);
    }
  }

  protected SchemaNegotiatorImpl newNegotiator() {
    try {
      return options.negotiatorClass().newInstance();
    } catch (ReflectiveOperationException e) {
      throw UserException.systemError(e)
        .build(ManagedScanFramework.logger);
    }
  }
}
