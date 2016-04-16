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

import org.apache.drill.yarn.core.Util;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract description of a remote process launch that describes the many
 * details needed to launch a process on a remote node.
 * <p>
 * Based on <a href="https://github.com/hortonworks/simple-yarn-app">Simple YARN App</a>.
 */

public class LaunchSpec {
  /**
   * List of (key, file) pairs to be localized to the node
   * before running the command. The file must exist in a distributed
   * file system (such as HDFS) visible to both the client and remote node.
   * Typically, the path is relative or absolute within the file system
   * defined by the fs.defaultFS parameter in core-site.xml.
   * <p>
   * TODO: Can the value also be a URL such as
   * <p>
   * <code>hdfs://somehost:1234//path/to/file
   * <p>
   * The key is used as (what?).
   */

  public Map<String, String> resources = new HashMap<>();

  /**
   * Defines environment variables to be set on the remote host before
   * launching the remote app. Note: do not set CLASSPATH here;
   * use {@link #classPath} instead.
   */

  public Map<String, String> env = new HashMap<>();

  /**
   * Set to the name of the OS command to run when we wish to run
   * a non-Java command.
   */

  public String command;

  /**
   * Set to the name of the Java main class (the one with the main method)
   * when we wish to run a Java command.
   */

  public String mainClass;

  /**
   * Set to the application-specific class path for the Java application.
   * These values are added to the Hadoop-provided values. These items
   * are relative to (what?), use (what variables) to refer to the localized
   * application directory.
   */

  public List<String> classPath = new ArrayList<>();

  /**
   * Optional VM arguments to pass to the JVM when running a Java class;
   * ignored when running an OS command.
   */

  public String vmArgs;

  /**
   * Arguments to the remote command.
   */

  public List<String> cmdArgs = new ArrayList<>();

  public LaunchSpec() {
  }

  public LaunchSpec(LaunchSpec from) {
    resources.putAll(from.resources);
    env.putAll(from.env);
    command = from.command;
    mainClass = from.mainClass;
    classPath.addAll(from.classPath);
    vmArgs = from.vmArgs;
    cmdArgs.addAll(from.cmdArgs);
  }

  /**
   * Create the command line to run on the remote node. The
   * command can either be a simple OS command (if the
   * {@link #command} member is set) or can be a Java
   * class (if the {@link #mainClass} member is set. If the command is
   * Java, then we pass along optional Java VM arguments.
   * <p>
   * In all cases we append arguments to the command itself, and
   * redirect stdout and stderr to log files.
   *
   * @return the complete command string
   */

  public String getCommand() {
    List<String> cmd = new ArrayList<>();
    if (command != null) {
      cmd.add(command);
    } else {
      assert mainClass != null;
      cmd.add("$JAVA_HOME/bin/java");
      if (vmArgs != null) {
        cmd.add(vmArgs);
      }
      cmd.add(mainClass);
    }
    cmd.addAll(cmdArgs);
    cmd.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
    cmd.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

    // Java 8
    // return String.join( " ", cmd );
    return Util.join(" ", cmd);
  }

  /**
   * Given this generic description of an application, create the detailed
   * YARN application submission context required to launch the
   * application.
   *
   * @param conf the YARN configuration obtained by reading the Hadoop
   *             configuration files
   * @return the completed application launch context for the given
   * application
   * @throws IOException if localized resources are not found in the
   *                     distributed file system (such as HDFS)
   */

  public ContainerLaunchContext createLaunchContext(YarnConfiguration conf) throws IOException {
    // Set up the container launch context
    ContainerLaunchContext amContainer =
            Records.newRecord(ContainerLaunchContext.class);

    // Set up the list of commands to run. Here, we assume that we run only
    // one command. The command can be null for an unmanaged (in-process)
    // launch.

    if (command != null || mainClass != null) {
      amContainer.setCommands(
              Collections.singletonList(getCommand()));
    }

    // Add localized resources
    Map<String, LocalResource> localResources = new HashMap<>();
    for (String key : resources.keySet()) {
      String resourcePath = resources.get(key);
      LocalResource localResource = setupLocalResource(conf, new Path(resourcePath));
      localResources.put(key, localResource);
    }
    amContainer.setLocalResources(localResources);

    // Setup the environment, with special handling for CLASSPATH

    List<String> fullClassPath = new ArrayList<>();
    setupStandardClassPath(conf, fullClassPath);
    fullClassPath.addAll(classPath);

    Map<String, String> appMasterEnv = new HashMap<String, String>();
    appMasterEnv.putAll(env);
    appMasterEnv.put(Environment.CLASSPATH.name(), Util.join(File.pathSeparator, fullClassPath));
    amContainer.setEnvironment(appMasterEnv);
    return amContainer;
  }

  /**
   * Create a local resource definition for YARN. A local resource is one that
   * must be localized onto the remote node prior to running a command on that
   * node.
   * <p>
   * YARN uses the size and timestamp are used to check if the file has changed on HDFS
   * to check if YARN can use an existing copy, if any.
   * <p>
   * Resources are made public.
   *
   * @param conf         Configuration created from the Hadoop config files,
   *                     in this case, identifies the target file system.
   * @param resourcePath the path (relative or absolute) to the file on the
   *                     configured file system (usually HDFS).
   * @return a YARN local resource records that contains information about
   * path, size, type, resource and so on that YARN requires.
   * @throws IOException if the resource does not exist on the configured
   *                     file system
   */

  private LocalResource setupLocalResource(YarnConfiguration conf, Path resourcePath) throws IOException {
    LocalResource resource = Records.newRecord(LocalResource.class);
    FileStatus fileStat = FileSystem.get(conf).getFileStatus(resourcePath);
    resource.setResource(ConverterUtils.getYarnUrlFromPath(resourcePath));
    resource.setSize(fileStat.getLen());
    resource.setTimestamp(fileStat.getModificationTime());
    resource.setType(LocalResourceType.FILE);

    // TODO: Make this an application resource instead?

    resource.setVisibility(LocalResourceVisibility.PUBLIC);
    return resource;
  }

  /**
   * Add standard class path entries from the YARN application class path
   * and the localized working directory.
   *
   * @param conf
   * @param fullClassPath
   */

  private void setupStandardClassPath(YarnConfiguration conf, List<String> fullClassPath) {
    String path[] = conf.getStrings(
            YarnConfiguration.YARN_APPLICATION_CLASSPATH,
            YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH);
    if (path == null) {
      return;
    }
    for (String item : path) {
      fullClassPath.add(item);
    }
    fullClassPath.add(ApplicationConstants.Environment.PWD.$() + File.separator + "*");
  }

}
