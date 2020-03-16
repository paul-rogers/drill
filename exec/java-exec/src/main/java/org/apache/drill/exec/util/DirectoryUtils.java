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

package org.apache.drill.exec.util;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;

public class DirectoryUtils {

  /**
   * Returns a path to Drill's temp directory. First checks to see if the variable DRILL_TMP_DIR is
   * set in the Drill configuration. If it is not, check the System environment to see if it is set there.
   * If it is set in neither location, returns empty string.
   * @param context
   * @return Path of Drill's temp directory
   * @throws Exception If the option is not set, or if there are some other issues, throw an exception
   */
  public static String getDrillTempDirectory(FragmentContext context) throws Exception {
    String drillTempDir;

    if (context.getOptions().getOption(ExecConstants.DRILL_TMP_DIR) != null) {
      drillTempDir = context
        .getOptions()
        .getOption(ExecConstants.DRILL_TMP_DIR)
        .string_val;
    } else {
      drillTempDir = System.getenv("DRILL_TMP_DIR");
    }
    return drillTempDir;
  }
}
