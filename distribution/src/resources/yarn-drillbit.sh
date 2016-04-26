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

# DRILL_HOME is set by the AM to point to the Drill distribution.

# In YARN, configuration files must be in the standard location.

DRILL_CONF_DIR=$DRILL_HOME/conf

# The log directory is YARN's container log directory

DRILL_LOG_DIR=$LOG_DIRS

# Use the YARN-provided JAVA_HOME

JAVA=$JAVA_HOME/bin/java

# Memory options should have been passed from the Application Master.

DRILL_MAX_DIRECT_MEMORY=${DRILL_MAX_DIRECT_MEMORY:-"8G"}
DRILL_HEAP=${DRILL_HEAP:-"4G"}

# JVM options set here are seledom (if every) customized per-site.
# If we find a need to customize any of these, we should add an
# additional Drill-on-YARN configuration variable for that item.

DRILL_JAVA_OPTS="-Xms$DRILL_HEAP -Xmx$DRILL_HEAP -XX:MaxDirectMemorySize=$DRILL_MAX_DIRECT_MEMORY -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=1G -Ddrill.exec.enable-epoll=true"

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

# Followed by Hadoop's classpath as provided by YARN.

CP=$CP:$CLASSPATH

# Followed by HBase's jar (or similar JARS defined in the Drill-on-YARN config.)
if [ -n "$DRILL_CLASSPATH" ]; then
  CP=$CP:$DRILL_CLASSPATH
fi

# Followed by Drill's other dependency jars
CP=$CP:$DRILL_HOME/jars/3rdparty/*
CP=$CP:$DRILL_HOME/jars/classb/*

# Log setup

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

# Set default scheduling priority
if [ "$DRILL_NICENESS" = "" ]; then
    export DRILL_NICENESS=0
fi

# The next message goes to YARN's container log file

echo starting drillbit, logging to $logout

# This message, and Drill messages, go to Drill's log file

echo "`date` Starting drillbit on `hostname` under YARN" >> $loglog

echo "`ulimit -a`" >> $loglog 2>&1
logopts="-Dlog.path=$DRILLBIT_LOG_PATH -Dlog.query.path=$DRILLBIT_QUERY_LOG_PATH"
if [ -n "$DRILL_LOG_GC" ]; then
	logopts="$logopts -Xloggc:${loggc}"
fi
jvmopts="$DRILL_JAVA_OPTS $SERVER_GC_OPTS $logopts"
bitcmd=$JAVA $jvmopts -cp $CP org.apache.drill.exec.server.Drillbit

# Debugging information

echo "Command: $bitcmd"
echo "Environment:"
env

# Launch Drill itself

nice -n $DRILL_NICENESS $bitcmd >> "$logout" 2>&1 &
bitpid=$!
wait $bitpid
retcode=$?

# Log to Drill and YARN logs
echo "`date` drillbit on `hostname` pid $bitpid exited with status $retcode" >> $loglog
echo "`date` drillbit on `hostname` pid $bitpid exited with status $retcode"

# Pass along Drill's exit code as our own.
 
exit $retcode