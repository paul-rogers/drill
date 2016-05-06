#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Launch script for the Drill Application Master (AM).
# This script runs under YARN and assumes the environment that YARN provides to an AM.
# This script likely will not work from the command line.

# YARN requires that the AM run as a child process until completion; so this script
# does not launch the AM in the background.

# This script is run from $DRILL_HOME/bin, wherever the user has configured it.

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`
DRILL_HOME=`cd "$bin/..">/dev/null; pwd`

# TODO: Change to use site dir when available.

DRILL_CONF_DIR=${DRILL_CONF_DIR:-$DRILL_HOME/conf}

if [ -n "$DRILL_DEBUG" ]; then
  echo
  echo "Drill AM launch script"
  echo "DRILL_HOME: $DRILL_HOME"
  echo "DRILL_CONF_DIR: $DRILL_CONF_DIR"
fi

# DRILL_AM_HEAP and DRILL_AM_JAVA_OPTS are set by the
# Drill client via YARN. To set these, use the following
# configuration options:
#
# DRILL_AM_HEAP: drill.yarn.am.heap
# DRILL_AM_JAVA_OPTS: drill.yarn.am.vm-args

DRILL_AM_HEAP=${DRILL_AM_HEAP:-"512M"}
AM_LOG_CONF="-Dlogback.configurationFile=${DRILL_HOME}/conf/drill-am-log.xml"

AM_JAVA_OPTS="-Xms$DRILL_AM_HEAP -Xmx$DRILL_AM_HEAP -XX:MaxPermSize=512M $DRILL_AM_JAVA_OPTS $AM_LOG_CONF"

# Build the class path.
# Start with Drill conf folder at the beginning of the classpath

CP=$DRILL_CONF_DIR

# Drill core jars
CP=$CP:$DRILL_HOME/jars/tools/*
CP=$CP:$DRILL_HOME/jars/*

# We do not use Hadoop's class path; it has dependencies which conflict
# with Drill. Instead, we use Drill's copies of the required Hadoop jars.
#CP=$CP:$CLASSPATH

# Do add Hadoop configuration and YARN jars directly,
# using the YARN-provided HADOOP_CONF_DIR and HADOOP_YARN_HOME.
CP=$CP:$HADOOP_CONF_DIR:$HADOOP_YARN_HOME/share/hadoop/yarn/*

# Drill override dependency jars
CP=$CP:$DRILL_HOME/jars/ext/*

# Drill other dependency jars
CP=$CP:$DRILL_HOME/jars/3rdparty/*
CP=$CP:$DRILL_HOME/jars/classb/*

# JAVA_HOME given by YARN

JAVA=$JAVA_HOME/bin/java

DRILL_DEBUG=1
if [ -n "$DRILL_DEBUG" ]; then
  echo "AM launch environment:"
  echo "-----------------------------------"
  env
  echo "-----------------------------------"
  echo $JAVA $AM_JAVA_OPTS -cp $CP org.apache.drill.yarn.appMaster.DrillApplicationMaster $@
fi

# Note: no need to capture output, YARN does that for us.
# AM is launched as a child process of caller, replacing this script.

# Replace this script process with the AM. Needed so that
# the YARN node manager can kill the AM if necessary by killing
# the PID for this script.

exec $JAVA $AM_JAVA_OPTS -cp $CP org.apache.drill.yarn.appMaster.DrillApplicationMaster $@
