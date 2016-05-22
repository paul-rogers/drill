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

# Amount of memory to use for Drillbits. Values are set by the Drill-on-YARN
# configuration when running under YARN. Drill provides default values of
# 8G direct, 4G heap if you don't specify values here. However, you should
# specify values that work for you.
#
# Keep the ${FOO:-your-value} notation so that these values are used only
# when the values are not passed in from YARN (or the environment.)

#export DRILL_MAX_DIRECT_MEMORY=${DRILL_MAX_DIRECT_MEMORY:-"8G"}
#export DRILL_HEAP=${DRILL_HEAP:-"4G"}

