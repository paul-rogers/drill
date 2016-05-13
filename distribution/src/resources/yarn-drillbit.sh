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
#
# Internal script to launch a Drillbit under YARN. Not for use outside
# of YARN.

# This is a heavily modified version of drillbit.sh, drill-config.sh and
# runbit, modified for use in YARN and performing a single operation:
# launching a Drillbit and waiting for Drillbit exit.
#
# Under YARN, the typical way to launch a Java app is to do all the
# setup in Java code in the launch context. However, Drill depends on
# drill-env.sh to set site-specific options before launch. This script
# performs Drill launch, integrating options from drill-env.sh.
#
# Input environment variables:
#
# DRILL_DEBUG
#     Used to debug this script. Dumps debugging information.
#     Set from the drill.yarn.drillbit.debug-launch config parameter.
# DRILL_HOME
#     Identifies the Drill software to use: either at a fixed
#     location, or localized. Set from the drill.yarn.drill-install
#     localize and drill-home config parameters. Note that this
#     variable tells this script where to find the drill-env.sh
#     file (in $DRILL_HOME/conf), and so DRILL_HOME cannot be
#     overridden in drill-env.sh.
# DRILL_LOG_DIR
#     The location to which to write log files. Often set in drill-env.sh.
#     Non-YARN launch provides a number of default locations. This variable
#     is ignored in a YARN launch if the drill.yarn.drillbit.yarn-logs is
#     true (the default.) Set the config variable to false to use this
#     value for the long directory under YARN.
# DRILL_YARN_LOG_DIR
#     If using the YARN log directory, this variable points to that location.
#     If drill.yarn.drillbit.yarn-logs is false, then this variable is not
#     set and the DRILL_LOG_DIR is used instead.
# DRILL_MAX_DIRECT_MEMORY:
#     The amount of direct memory set in the
#     drill.yarn.drillbit.max-direct-memory config parameter.
#     When Drill is run outside of YARN, this value is set in drill-env.sh.
#     But doing so under YARN decouples the Drill memory settings from the
#     YARN settings. If you do not set the config parameter, Drill will
#     default to the value set in drill-env.sh.
# DRILL_HEAP
#     The amount of Java heap memory set in the
#     drill.yarn.drillbit.heap setting. Same override rules as
#     DRILL_MAX_DIRECT_MEMORY.
# DRILL_JAVA_OPTS
#     The standard JVM options needed to launch Drill. Must be set in
#     drill-env.sh. Additional values can be set in the
#     drill.yarn.drillbit.vm-args config parameter.
# SERVER_GC_OPTS
#     Additional garbage collection (GC) related JVM options set
#     in drill-env.sh.
# HADOOP_HOME
#     Location of the Hadoop software and configuration. Can be
#     set with the drill.yarn.hadoop.home or in drill-env.sh. If both are set, the
#     latter value overrides the former.
# JAVA_HOME
#     Set by YARN, bypassing Drill's usual process for searching for JAVA_HOME.
# DRILL_CONF_DIR
#     Location of Drill's configuration files. Non-YARN launch provides a set of
#     defaults. Under YARN, this value must either be set explicitly using the
#     drill.yarn.drillbit.conf-dir parameter, or will default to
#     the (typically localized) $DRILL_HOME/conf directory.
# DRILL_CLASSPATH_PREFIX
#     Optional extra classpath added before Drill's own jars. Set from the
#     drill.yarn.drillbit.prefix-class-path config parameter, or in
#     drill-env.sh. As with all class path settings, if both values are set,
#     drill-env.sh takes precedence.
# EXTN_CLASSPATH
#     Optional extra classpath added after Drill's own jars but before
#     3rd party jars. Allows overriding Drill's bundled version of Hadoop
#     and so on. Allows adding jars needed by plug-ins. Config parameter
#     is drill.yarn.drillbit.extn-class-path.
# HADOOP_CLASSPATH
#     As above, but for the Hadoop class path. Config parameter is
#     drill.yarn.drillbit.hadoop-class-path. This is a legacy setting. Use
#     drill.yarn.drillbit.extn-class-path for new deployments.
# HBASE_CLASSPATH
#     As above, but for the Hbase class path. Config parameter is
#     drill.yarn.drillbit.hbase-class-path. This is a legacy setting. Use
#     drill.yarn.drillbit.extn-class-path for new deployments.
# DRILL_CLASSPATH
#     Optional extra classpath after all Drill-provided jars. This is the
#     typical place to add jars needed by plugins, etc. (Note, no need to set
#     this if the jars reside in the $DRILL_HOME/jars/3rdparty directory.)
#     Config parameter is drill.yarn.drillbit.drill-classpath.

# DRILL_HOME is set by the AM to point to the Drill distribution.

# In YARN, configuration defaults to the the standard location.

DRILL_CONF_DIR={$DRILL_CONF_DIR:$DRILL_HOME/conf}

# Use Drill's standard configuration, including drill-env.sh.

. "$DRILL_CONF_DIR/drill-config.sh"

if [ -n "$DRILL_DEBUG" ]; then
  echo
  echo "Drillbit Environment from YARN:"
  echo "-----------------------------------"
  env
  echo "-----------------------------------"
fi

# The log directory is YARN's container log directory

DRILL_LOG_DIR=$LOG_DIRS

# Use the YARN-provided JAVA_HOME

JAVA=$JAVA_HOME/bin/java

# Memory options should have been passed from the Application Master.

DRILL_MAX_DIRECT_MEMORY=${DRILL_MAX_DIRECT_MEMORY:-"8G"}
DRILL_HEAP=${DRILL_HEAP:-"4G"}

# JVM options set here are seldom (if every) customized per-site.
# If we find a need to customize any of these, we should add an
# additional Drill-on-YARN configuration variable for that item.

JVM_OPTS="-Xms$DRILL_HEAP -Xmx$DRILL_HEAP -XX:MaxDirectMemorySize=$DRILL_MAX_DIRECT_MEMORY -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=1G -Ddrill.exec.enable-epoll=true"

# Class unloading is disabled by default in Java 7
# http://hg.openjdk.java.net/jdk7u/jdk7u60/hotspot/file/tip/src/share/vm/runtime/globals.hpp#l1622
SERVER_GC_OPTS="-XX:+CMSClassUnloadingEnabled -XX:+UseG1GC "

# Class path
# Note: Custom user code must appear in jars/3rdparty.

# Add Drill conf folder at the beginning of the classpath
CP=$DRILL_CONF_DIR

# Next Drill core jars
CP=$CP:$DRILL_HOME/jars/*

# Followed by Drill override dependency jars
CP=$CP:$DRILL_HOME/jars/ext/*

# Followed by jars defined in the Drill-on-YARN config.)
if [ -n "$DRILL_CLASSPATH" ]; then
  CP=$CP:$DRILL_CLASSPATH
fi

# Followed by Drill's other dependency jars
# Note that Hadoop jars are included in 3rdpaty; we use these to
# avoid potential conflicts with the YARN-provided jars, but it means
# that the Drill version must be built with the correct Hadoop version.

CP=$CP:$DRILL_HOME/jars/3rdparty/*
CP=$CP:$DRILL_HOME/jars/classb/*

# Log setup
# In "native" Drill, stdout goes to a Drill log file.
# Under YARN, the script that launched this one already captures
# output to YARN's own logs, so we don't do our own capture here.

DRILL_LOG_PREFIX=drillbit
DRILL_LOGFILE=$DRILL_LOG_PREFIX.log
DRILL_OUTFILE=$DRILL_LOG_PREFIX.out
DRILL_QUERYFILE=${DRILL_LOG_PREFIX}_queries.json
loggc=$DRILL_LOG_DIR/$DRILL_LOG_PREFIX.gc
loglog="${DRILL_LOG_DIR}/${DRILL_LOGFILE}"
logout="${DRILL_LOG_DIR}/${DRILL_OUTFILE}"
logqueries="${DRILL_LOG_DIR}/${DRILL_QUERYFILE}"
DRILLBIT_LOG_PATH=$loglog
DRILLBIT_QUERY_LOG_PATH=$logqueries

# Note: no log rotation because each run under YARN
# gets a new log directory.

echo "`ulimit -a`" >> $loglog 2>&1
logopts="-Dlog.path=$DRILLBIT_LOG_PATH -Dlog.query.path=$DRILLBIT_QUERY_LOG_PATH"
if [ -n "$DRILL_LOG_GC" ]; then
  logopts="$logopts -Xloggc:${loggc}"
fi
JVM_OPTS="$JVM_OPTS $SERVER_GC_OPTS $logopts $DRILL_JAVA_OPTS"
export BITCMD="$JAVA $JVM_OPTS -cp $CP org.apache.drill.exec.server.Drillbit"

# Debugging information

if [ -n "$DRILL_DEBUG" ]; then
  echo "Command: $BITCMD"
  echo
  echo "Local Environment:"
  echo "-----------------------------------"
  set
  echo "-----------------------------------"
  echo "Script pid: $$"
fi

# Launch Drill using the same pid as this script.
# This configuration is necessary so that when the node manager
# kills this process, it kills the drillbit itself.

echo "`date` Starting drillbit on `hostname` under YARN, logging to $logout"

exec $BITCMD
#retcode=$?

#echo "`date` drillbit on `hostname` pid $bitpid exited with status $retcode" >> $loglog
#echo "`date` drillbit on `hostname` pid $bitpid exited with status $retcode"

# Pass along Drill's exit code as our own.
 
#exit $retcode
