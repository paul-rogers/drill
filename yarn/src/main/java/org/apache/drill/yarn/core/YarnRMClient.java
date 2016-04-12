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
package org.apache.drill.yarn.core;

import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;

import java.io.IOException;

/**
 * YARN resource manager client implementation for Drill. Provides a wrapper around
 * the YARN client interface to the Resource Manager. Used by the client app to
 * start the Drill application master.
 * <p>
 * Based on <a href="https://github.com/hortonworks/simple-yarn-app">simple-yarn-app</a>
 */

public class YarnRMClient {
  YarnConfiguration conf;
  YarnClient yarnClient;
  ApplicationId appId;

  public YarnRMClient() {
    this(new YarnConfiguration());
  }

  public YarnRMClient(YarnConfiguration conf) {
    this.conf = conf;
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();
  }

  public void launchAppMaster(AppSpec spec) throws YarnClientException {
    // Create application via yarnClient
    // Response is a new application ID along with cluster capacity info

    YarnClientApplication app;
    try {
      app = yarnClient.createApplication();
    } catch (YarnException | IOException e) {
      throw new YarnClientException("Create application failed", e);
    }
    GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
    System.out.println("Application ID: " + appResponse.getApplicationId().toString());
    System.out.println("Max Memory: " + appResponse.getMaximumResourceCapability().getMemory());
    System.out.println("Max Cores: " + appResponse.getMaximumResourceCapability().getVirtualCores());

    ApplicationSubmissionContext appContext;
    try {
      appContext = spec.createAppLaunchContext(conf, app);
    } catch (IOException e) {
      throw new YarnClientException("Create app launch context failed", e);
    }

    // Submit application
    appId = appContext.getApplicationId();
    System.out.println("Submitting application " + appId);
    try {
      yarnClient.submitApplication(appContext);
    } catch (YarnException | IOException e) {
      throw new YarnClientException("Submit application failed", e);
    }
  }

  public ApplicationId getAppId() {
    return appId;
  }

  public ApplicationReport getAppReport() throws YarnClientException {
    try {
      return yarnClient.getApplicationReport(appId);
    } catch (YarnException | IOException e) {
      throw new YarnClientException("Get application report failed", e);
    }
  }

  /**
   * Waits for the application to start. This version is somewhat informal,
   * the intended use is when debugging unmanaged applications.
   *
   * @throws YarnClientException
   */
  public ApplicationAttemptId waitForStart() throws YarnClientException {
    ApplicationReport appReport;
    YarnApplicationState appState;
    ApplicationAttemptId attemptId;
    for (; ; ) {
      appReport = getAppReport();
      appState = appReport.getYarnApplicationState();
      attemptId = appReport.getCurrentApplicationAttemptId();
      if (appState != YarnApplicationState.NEW &&
              appState != YarnApplicationState.NEW_SAVING &&
              appState != YarnApplicationState.SUBMITTED) {
        break;
      }
      System.out.println("App State: " + appState);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // Should never occur.
      }
    }
    if (appState != YarnApplicationState.ACCEPTED) {
      throw new YarnClientException("Application start failed with status " + appState);
    }

    return attemptId;
  }

  /**
   * Wait for the application to enter one of the completion states. This is an informal
   * implementation useful for testing.
   *
   * @throws YarnClientException
   */

  public void waitForCompletion() throws YarnClientException {
    ApplicationReport appReport;
    YarnApplicationState appState;
    for (; ; ) {
      appReport = getAppReport();
      appState = appReport.getYarnApplicationState();
      if (appState == YarnApplicationState.FINISHED ||
              appState == YarnApplicationState.KILLED ||
              appState == YarnApplicationState.FAILED) {
        break;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        // Should never occur.
      }
    }

    System.out.println(
            "Application " + appId + " finished with" +
                    " state " + appState +
                    " at " + appReport.getFinishTime());
  }

  public Token<AMRMTokenIdentifier> getAMRMToken() throws YarnClientException {
    try {
      return yarnClient.getAMRMToken(appId);
    } catch (YarnException | IOException e) {
      throw new YarnClientException("Get AM/RM token failed", e);
    }
  }
}
