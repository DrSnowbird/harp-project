#
# Copyright 2014 Indiana University
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#!/bin/bash

#Setting local classpath.
cp=$HARP_HOME/scripts
for i in ${HARP_HOME}/lib/*.jar;
  do cp=$i:${cp}
done

#echo $cp
java -Xmx2048m -Xms512m -XX:NewRatio=1 -XX:SurvivorRatio=18 -XX:+UseParallelGC -XX:-UseParallelOldGC -XX:+AggressiveOpts -classpath $cp edu.iu.harp.depl.QuickDeployment

