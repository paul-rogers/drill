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

# Preliminary version

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`
DRILL_HOME=`cd "$bin/..">/dev/null; pwd`

# TODO: Change to use site dir when available.

DRILL_CONF_DIR=${DRILL_CONF_DIR:-$DRILL_HOME/conf}
DRILL_LOG_DIR=${DRILL_LOG_DIR:-$LOG_DIRS}

# Start with YARN and Hadoop-provided class path

CP=$CLASSPATH

# Add Drill conf folder at the beginning of the classpath
CP=$CP:$DRILL_CONF_DIR

# Drill core jars
CP=$CP:$DRILL_HOME/jars/*

# Drill override dependency jars
CP=$CP:$DRILL_HOME/jars/ext/*

# Drill other dependency jars
CP=$CP:$DRILL_HOME/jars/3rdparty/*
CP=$CP:$DRILL_HOME/jars/classb/*

# JAVA_HOME given by YARN

JAVA=$JAVA_HOME/bin/java

# DRILL_JAVA_OPTS is optional, if needed, is set in the AM launch
# context.

# Note: no need to capture output, YARN does that for us.
# AM is launched as a child process of caller, replacing this script.

exec $JAVA $DRILL_JAVA_OPTS -cp $CP org.apache.drill.yarn.appMaster.ApplicationMaster $@

