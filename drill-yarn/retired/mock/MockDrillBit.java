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
package org.apache.drill.yarn.mock;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

public class MockDrillBit {

  static int crashProbability = 50;
  static int lifeTimeSec = 15;

  public static void main(String[] args) {
    String containerId = System.getenv("CONTAINER_ID");
    if (containerId == null) {
      containerId = "test";
    }
    if (args.length > 0) {
      crashProbability = Integer.parseInt(args[1]);
    }

    File monitorFile = new File("/tmp");
    monitorFile = new File(monitorFile, containerId + ".kill");
    monitorFile.delete();

    double rand = Math.random() * 100;
    boolean shouldFail = (rand < crashProbability);
    shouldFail = false;
    String msg = "Mock Drill bit " + containerId;
    if (shouldFail) {
      msg += " - will crash";
    }
    append(msg);

    for (int count = 0; true; count++) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // Won't happen
      }
      if (shouldFail && count > lifeTimeSec) {
        append("Mock Drill bit " + containerId + " - failing now.");
        System.exit(-1);
      }
      if (monitorFile.exists()) {
        append("Mock Drill bit " + containerId + " - exiting.");
        break;
      }
    }
  }

  public static void append(String msg) {
    File logFile = new File("/tmp/mock-bit.log");
    try {
      FileOutputStream os = new FileOutputStream(logFile, true);
      try {
        java.nio.channels.FileLock lock = os.getChannel().lock();
        PrintWriter out = null;
        try {
          out = new PrintWriter(os);
          out.println(msg);
        } finally {
          lock.release();
          if (out != null) {
            out.close();
            os = null;
          }
        }
      } finally {
        if (os != null) {
          os.close();
        }
      }
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
