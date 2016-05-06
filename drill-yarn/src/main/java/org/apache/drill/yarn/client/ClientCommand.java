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
package org.apache.drill.yarn.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.drill.yarn.core.DrillOnYarnConfig;
import org.apache.drill.yarn.core.YarnRMClient;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;

public abstract class ClientCommand
{
  protected CommandLineOptions opts;

  public void setOpts(CommandLineOptions opts) {
    this.opts = opts;
  }

  public abstract void run( ) throws ClientException;

  /**
   * Return the path to the app id file.
   * The file goes into the directory above Drill Home (which should be the
   * folder that contains the localized archive) and is named for the
   * ZK cluster (to ensure that the name is a valid file name.)
   * @return
   */
  protected static File getAppIdFile( ) {
    String clusterId = DrillOnYarnConfig.config().getString( DrillOnYarnConfig.CLUSTER_ID );
    String appIdFileName = clusterId + ".appid";
    File drillHome = DrillOnYarnConfig.instance().getLocalDrillHome( );
    File appIdFile = new File( drillHome.getParentFile(), appIdFileName );
    return appIdFile;
  }

  protected ApplicationId checkAppId( ) throws ClientException {
    File appIdFile = getAppIdFile( );
    ApplicationId appId = loadAppId( appIdFile );
    if ( appId == null ) {
      throw new ClientException( "No Drill cluster is running (did not find file appid file: " + appIdFile.toString( ) + ")" );
    }
    return appId;
  }

  protected YarnRMClient getClient( ) throws ClientException {
    return new YarnRMClient( checkAppId( ) );
  }

  protected ApplicationId loadAppId( File appIdFile ) {
    BufferedReader reader = null;
    String appIdStr;
    try {
      reader = new BufferedReader( new FileReader( appIdFile ) );
      appIdStr = reader.readLine();
      if ( appIdStr != null ) {
        appIdStr = appIdStr.trim( );
      }
    } catch (FileNotFoundException e) {
      return null;
    } catch (IOException e) {
      return null;
    }
    finally {
      try {
        reader.close( );
      } catch (IOException e) {
        // Ignore
      }
    }
    return ConverterUtils.toApplicationId(appIdStr);
  }

}
