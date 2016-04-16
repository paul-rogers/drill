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

# Run the Drill-on-YARN client to launch Drill on a YARN cluster.
# Uses the $DRILL_HOME/conf/drill-on-yarn.conf file for client options.
# Uses the $DRILL_HOME/conf/drill-cluster.conf file for cluster options.
#
# The config files (along with the Drill config files) MUST be in the
# $DRILL_HOME directory so that they are properly localized. Drill-on-YARN does not permit
# placing configuration files outside the $DRILL_HOME directory.
#
# Requires the location of Hadoop home. Maybe passed using the --hadoop option,
# set in the environment, or set in $DRILL_HOME/conf/yarn-env.sh.

usage="Usage: drill-on-yarn.sh\
 [--hadoop <yarn-home>]\
 (start)"

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`
DRILL_HOME=`cd "$bin/..">/dev/null; pwd`

while [[ $# > 0 ]]; do
    key="$1"

    case $key in
    -y|--hadoop)
        YARN_DIR="$2"
        shift 2
        ;;
    -h|-\?|--help)
        echo $usage
        echo "Config file defaults to $DRILL_HOME/conf/drill-on-yarn.conf"
        echo "Config file and Hadoop home can be set in $DRILL_HOME/conf/yarn-env.sh"
        exit 0
    ;;
    start|debug)
        cmd="$key"
        shift
        ;;
    *)
        echo $usage
        exit 1
    ;;
    esac
done

# if no command specified, show usage
if [ -z "$cmd" ]; then
  echo $usage
  exit 1
fi

# Use Drillbit's config script. We throw away most of the information, we really just
# need JAVA_HOME.
. "$bin"/drill-config.sh

# Load Drill-on-YARN client environment configuration.
. "$DRILL_HOME/conf/yarn-env.sh"

# Hadoop Home is required
YARN_DIR=${YARN_DIR:-$HADOOP_HOME}
if [ -z "$YARN_DIR" ]; then
    echo "Hadoop home undefined: set HADOOP_HOME or provide --hadoop option" >&2
    exit 1
fi

JAVA=$JAVA_HOME/bin/java

# Custom class-path

# Add Drill conf folder at the beginning of the classpath
CP=$DRILL_CONF_DIR

# Next Drill core jars, including Drill-on-YARN
CP=$CP:$DRILL_HOME/jars/*

# Followed by Drill override dependency jars
CP=$CP:$DRILL_HOME/jars/ext/*

# Followed by YARN's and Hadoop's jars
CP=$CP:$HADOOP_HOME/share/hadoop/yarn/*
CP=$CP:$HADOOP_HOME/share/hadoop/common/*
CP=$CP:$HADOOP_HOME/share/hadoop/hdfs/*

# Followed by Drill other dependency jars
CP=$CP:$DRILL_HOME/jars/3rdparty/*
CP=$CP:$DRILL_HOME/jars/classb/*

case $cmd in
start)
    exec $JAVA -cp $CP org.apache.drill.yarn.client.Client $cmd
;;
debug)
    env
    echo "Command: $JAVA -cp $CP org.apache.drill.yarn.client.Client $cmd"
;;
*)
    echo $usage
    exit 1
;;
esac

