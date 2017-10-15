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
package org.apache.drill.exec.physical.impl.scan.basic;

import org.apache.drill.exec.physical.impl.scan.managed.AbstractReaderShim;
import org.apache.drill.exec.physical.impl.scan.managed.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.managed.SchemaNegotiator;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;

public class BasicReaderShim extends AbstractReaderShim<SchemaNegotiator> {

  public BasicReaderShim(BasicScanFramework framework, ManagedReader<SchemaNegotiator> reader) {
    super(framework, reader);
  }

  @Override
  protected boolean openReader() {
    SchemaNegotiator schemaNegotiator = new BasicSchemaNegotiatorImpl(manager.context(), this);
    return reader.open(schemaNegotiator);
  }

  public ResultSetLoader build(BasicSchemaNegotiatorImpl schemaNegotiator) {
    manager.projector().startFile(null);
    return super.build(schemaNegotiator);
  }
}
