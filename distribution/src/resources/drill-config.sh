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
# included in all the drill scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*
# Modelled after $HADOOP_HOME/bin/hadoop-config.sh
#
# Environment Variables:
#
#   DRILL_HOME                 Drill home (defaults based on this
#                              script's path.)
#
#   JAVA_HOME                  The java implementation to use.
#
#   DRILL_CLASSPATH            Extra Java CLASSPATH entries.
#
#   DRILL_CLASSPATH_PREFIX     Extra Java CLASSPATH entries that should
#                              be prefixed to the system classpath.
#
#   HADOOP_HOME                Hadoop home
#
#   HBASE_HOME                 HBase home

# resolve links - "${BASH_SOURCE-$0}" may be a softlink
this="${BASH_SOURCE-$0}"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
home=`cd "$bin/..">/dev/null; pwd`
this="$home/bin/$script"

# Standardize error messages

fatal_error() {
  echo "ERROR: $@" 1>&2
  exit 1
}

drill_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
    num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
    while [ $num -gt 1 ]; do
        prev=`expr $num - 1`
        [ -f "$log.$prev" ] && mv -f "$log.$prev" "$log.$num"
        num=$prev
    done
    mv -f "$log" "$log.$num";
    fi
}

# the root of the drill installation
DRILL_HOME=${DRILL_HOME:-$home}

# Check to see if the conf dir or drill home are given as an optional arguments
# Must be the first arguments on the command line.

if [ "--config" = "$1" ]; then
  shift
  DRILL_CONF_DIR=$1
  shift
fi

# If config dir is given, it must exist.

if [ -n "$DRILL_CONF_DIR" ]; then
  if [[ ! -d "$DRILL_CONF_DIR" ]]; then
    fatal_error "Config dir does not exist:" $DRILL_CONF_DIR
  fi
else

  # Allow alternate drill conf dir location.
  DRILL_CONF_DIR=${DRILL_CONF_DIR:-/etc/drill/conf}

  # Otherwise, use the default
  if [[ ! -d "$DRILL_CONF_DIR" ]]; then
    DRILL_CONF_DIR="$DRILL_HOME/conf"
  fi
fi

# However we got the config dir, it must contain a config
# file, and that file must be readable.

drillenv="$DRILL_CONF_DIR/drill-env.sh"
if [[ ! -a "$drillenv" ]]; then
  fatal_error "Drill config file missing: $drillenv -- Wrong config dir?"
fi
if [[ ! -r "$drillenv" ]]; then
  fatal_error "Drill config file not readable: $drillenv - Wrong user?"
fi

# Source drill-env.sh for any user configured values
. "$drillenv"

# Under YARN, the log directory is usually YARN-provided. Replace any
# value that may have been set in drill-env.sh.

if [ -n "$DRILL_YARN_LOG_DIR" ]; then
  DRILL_LOG_DIR="$DRILL_YARN_LOG_DIR"
fi

# Get log directory
if [ -z "$DRILL_LOG_DIR" ]; then
  # Try the optional location
  DRILL_LOG_DIR=/var/log/drill
  if [[ ! -d "$DRILL_LOG_DIR" && ! -w "$DRILL_LOG_DIR" ]]; then
    # Default to the drill home folder. Create the directory
    # if not present.

    DRILL_LOG_DIR=$DRILL_HOME/log
  fi
fi

# Regardless of how we got the directory, it must exist
# and be writable.

mkdir -p "$DRILL_LOG_DIR"
if [[ ! -d "$DRILL_LOG_DIR" && ! -w "$DRILL_LOG_DIR" ]]; then
  fatal_error "Log directory does not exist or is not writable: $DRILL_LOG_DIR"
fi

# Class path construction.

# Add Drill conf folder at the beginning of the classpath
CP=$DRILL_CONF_DIR

# Followed by any user specified override jars
if [ -n "${DRILL_CLASSPATH_PREFIX}" ]; then
  CP=$CP:$DRILL_CLASSPATH_PREFIX
fi

# Next Drill core jars
CP=$CP:$DRILL_HOME/jars/*

# Followed by Drill override dependency jars
CP=$CP:$DRILL_HOME/jars/ext/*

# Followed by Hadoop's jar
if [ -n "${HADOOP_CLASSPATH}" ]; then
  CP=$CP:$HADOOP_CLASSPATH
fi

# Followed by HBase' jar
if [ -n "${HBASE_CLASSPATH}" ]; then
  CP=$CP:$HBASE_CLASSPATH
fi

# Generalized extension path (use this for new deployments instead
# of the specialized HADOOP_ and HBASE_CLASSPATH variables.)
if [ -n "${EXTN_CLASSPATH}" ]; then
  CP=$CP:$EXTN_CLASSPATH
fi

# Followed by Drill other dependency jars
CP=$CP:$DRILL_HOME/jars/3rdparty/*
CP=$CP:$DRILL_HOME/jars/classb/*

# Finally any user specified
if [ -n "${DRILL_CLASSPATH}" ]; then
  CP=$CP:$DRILL_CLASSPATH
fi

# Test for cygwin
is_cygwin=false
case "`uname`" in
CYGWIN*) is_cygwin=true;;
esac

# Test for or find JAVA_HOME
if [ -z "$JAVA_HOME" ]; then
  if [ -e `which java` ]; then
    SOURCE=`which java`
    while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
      DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
      SOURCE="$(readlink "$SOURCE")"
      [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
    done
    JAVA_HOME="$( cd -P "$( dirname "$SOURCE" )" && cd .. && pwd )"
  fi
  # if we didn't set it
  if [ -z "$JAVA_HOME" ]; then
    fatal_error "JAVA_HOME is not set and Java could not be found"
  fi
fi
# Now, verify that 'java' binary exists and is suitable for Drill.
if $is_cygwin; then
  JAVA_BIN="java.exe"
else
  JAVA_BIN="java"
fi
JAVA=`find -L "$JAVA_HOME" -name $JAVA_BIN -type f | head -n 1`
if [ ! -e "$JAVA" ]; then
  fatal_error "Java not found at JAVA_HOME=$JAVA_HOME."
fi

# Ensure that Java version is at least 1.7
"$JAVA" -version 2>&1 | grep "version" | egrep -e "1.4|1.5|1.6" > /dev/null
if [ $? -eq 0 ]; then
  fatal_error "Java 1.7 or later is required to run Apache Drill."
fi

# Adjust paths for CYGWIN
if $is_cygwin; then
  DRILL_HOME=`cygpath -w "$DRILL_HOME"`
  DRILL_CONF_DIR=`cygpath -w "$DRILL_CONF_DIR"`
  DRILL_LOG_DIR=`cygpath -w "$DRILL_LOG_DIR"`
  CP=`cygpath -w -p "$CP"`
  if [ -z "$HADOOP_HOME" ]; then
    HADOOP_HOME=${DRILL_HOME}/winutils
  fi
fi

# make sure allocator chunks are done as mmap'd memory (and reduce arena overhead)
# Newer versions of glibc use an arena memory allocator that causes virtual
# memory usage to explode. Tune the variable down to prevent vmem explosion.
export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-4}
export MALLOC_MMAP_THRESHOLD_=131072
export MALLOC_TRIM_THRESHOLD_=131072
export MALLOC_TOP_PAD_=131072
export MALLOC_MMAP_MAX_=65536

# Variables exported form this script
export is_cygwin
export DRILL_HOME
export DRILL_CONF_DIR
export DRILL_LOG_DIR
export CP
export JAVA_HOME
export JAVA
