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

if [ $# -lt 1 ]; then
    echo Usage: command [parameters]
    echo Supported commands: 
    echo "initdir - Create a directory in all compute nodes (let's call this "data" directory)."
    echo "mkdir   - Create a sub directory inside "data" directory in all compute nodes."
    echo "rmdir   - Remove a sub directory inside "data" directory in all compute nodes."
    echo "put     - Distribute input data across compute nodes."
    echo "cpj     - Copy application jar files to all compute nodes."
    exit -1
fi

#Setting local classpath.
cp=$HARP_HOME/bin
for i in ${HARP_HOME}/lib/*.jar;
  do cp=$i:${cp}
done

case $1 in 'initdir')
  if [ $# -ne 2 ]; then
    echo Usage: initdir [Directory to create - complete path to the directory]
    exit -1
  fi
  java -Xmx512m -Xms256m -XX:-UseConcMarkSweepGC -XX:+UseParNewGC -XX:+AggressiveOpts -classpath $cp edu.iu.harp.dfs.DirManager $1 $2
       ;;

'mkdir')
  if [ $# -ne 2 ]; then
    echo Usage: mkdir [sub directory to create - relative to data_dir specified in harp.properties]
    exit -1
  fi 
  java -Xmx512m -Xms256m -XX:-UseConcMarkSweepGC -XX:+UseParNewGC -XX:+AggressiveOpts -classpath $cp edu.iu.harp.dfs.DirManager $1 $2
  ;;

'rmdir')
  if [ $# -ne 2 ]; then
    echo Usage: rmdir [sub directory to delete - - relative to data_dir specified in harp.properties]
    exit -1
  fi
  java -Xmx512m -Xms256m -XX:-UseConcMarkSweepGC -XX:+UseParNewGC -XX:+AggressiveOpts -classpath $cp edu.iu.harp.dfs.DirManager $1 $2
  ;;

'put')
  if [ $# -gt 6 -o $# -lt 5 ]; then
    echo Usage: put [input data directory \(local\)][destination directory \(remote\)][file filter][partition file][num replications \(optional\)]
    echo destination directory - relative to data_dir specified in harp.properties
    exit -1
  fi     
  java -Xmx512m -Xms256m -XX:-UseConcMarkSweepGC -XX:+UseParNewGC -XX:+AggressiveOpts -classpath $cp edu.iu.harp.dfs.FileDistributor $2 $3 $4 $5 $6
  ;;

'cpj')
  if [ $# -ne 2 ]; then
    echo 1>&2 Usage: [resource to copy to the apps directory]
    exit -1
  fi
  java -Xmx512m -Xms256m -XX:-UseConcMarkSweepGC -XX:+UseParNewGC -XX:+AggressiveOpts -classpath $cp edu.iu.harp.dfs.JarDistributor $2
  ;;
esac
