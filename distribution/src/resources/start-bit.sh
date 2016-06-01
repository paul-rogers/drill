#!/usr/bin/env bash
# Copyright 2013 The Apache Software Foundation
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Wrapper script to launch a drillbit as a child process and capture
# its pid. Waits for the Drillbit to exit before returning.
# Called from drillbit.sh with nohup to launch the drillbit in
# the background. Not meant to be invoked directly.

# Add to the command log file vital stats on our environment.

echo "`date` Starting $command on `hostname`" >> "$DRILLBIT_LOG_PATH"
echo "`ulimit -a`" >> "$DRILLBIT_LOG_PATH" 2>&1
nice -n $DRILL_NICENESS "$DRILL_HOME/bin/runbit" exec >> "$logout" 2>&1 &
echo $! > $pid
wait
