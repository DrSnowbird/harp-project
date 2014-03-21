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

if [ $# != 3 ]; then
    echo Usage: ./scptarxfile.sh [local file path][remote IP][remote Dir]
    exit -1
fi

path=$1
remoteIP=$2
remoteDir=$3

echo "Copying $path to $remoteIP:$remoteDir"
#echo scp $path $remoteIP:$remoteDir
scp $path $remoteIP:$remoteDir
file=${path##*/}
#echo ssh $remoteIP tar zxf $remoteDir/$file -C $remoteDir/ &
ssh $remoteIP tar zxf $remoteDir/$file -C $remoteDir/ &
