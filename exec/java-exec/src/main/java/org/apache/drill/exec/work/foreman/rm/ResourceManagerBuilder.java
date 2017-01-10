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
package org.apache.drill.exec.work.foreman.rm;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.local.LocalClusterCoordinator;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.SystemOptionManager;

public class ResourceManagerBuilder {

  private DrillbitContext context ;

  public ResourceManagerBuilder(final DrillbitContext context) {
    this.context = context;
  }

  public ResourceManager build() {
    ClusterCoordinator coord = context.getClusterCoordinator();
    DrillConfig config = context.getConfig();
    SystemOptionManager systemOptions = context.getOptionManager();
    if (coord instanceof LocalClusterCoordinator) {
      if (config.getBoolean(EmbeddedQueryQueue.ENABLED)) {
        return new BasicACResourceManager(context, new EmbeddedQueryQueue(context));
      }
    } else {
      if (systemOptions.getOption(ExecConstants.ENABLE_QUEUE)) {
        return new BasicACResourceManager(context, new DistributedQueryQueue(context));
      }
    }
    return new DefaultResourceManager();
  }
}
