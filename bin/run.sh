#!/usr/bin/env bash
#/**
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

# for debug
set -x

# install jdk
bk_basic=/home/web_server/bookkeeper
jdk_dirname=$bk_basic/jdk-17
wget -q https://halo.corp.kuaishou.com/api/cloud-storage/v1/public-objects/bop/jdk-17_linux-x64_bin.tar.gz
if [ ! -d "$bk_basic" ]; then
  mkdir -p "$bk_basic"
fi
if [ -d "$jdk_dirname" ]; then
  rm -rf "$jdk_dirname"
fi

tar -xzf jdk-17_linux-x64_bin.tar.gz
mv jdk-17.* $jdk_dirname
rm jdk-17_linux-x64_bin.tar.gz

# link
if [ -L "/home/web_server/bookkeeper/apps" ]; then
    rm /home/web_server/bookkeeper/apps
fi
ln -s /data/web_server/project/kuaishou-runner-apps/"$KWS_SERVICE_NAME"/code/bookkeeper-server /home/web_server/bookkeeper/apps

if [ -d "/home/web_server/bookkeeper/supervisord" ]; then
    rm /home/web_server/bookkeeper/supervisord
fi
ln -s /data/web_server/supervisord/conf /home/web_server/bookkeeper/supervisord

## save ENV vars
echo "export KWS_SERVICE_NAME=$KWS_SERVICE_NAME" > /data/web_server/project/kuaishou-runner-apps/"$KWS_SERVICE_NAME"/code/bookkeeper-server/conf/kwai_runtime.sh

## set directMem
directMem="80g"
# set heap size
heapSize="20g"
if [[ $KWS_SERVICE_NAME == *kop* ]]; then
  directMem="60g"
  heapSize="12g"
  # install python kconf
  pip3 install infra-kconf
  # generate cluster config
  python3 /home/web_server/bookkeeper/apps/bin/init_cluster_config.py
fi

export BOOKIE_MEM_OPTS="-Xms${heapSize} -Xmx${heapSize} -XX:MaxDirectMemorySize=${directMem} -XX:-UseNUMA"
export BOOKIE_ROOT_LOG_APPENDER="ROLLINGFILE"
export BOOKIE_LOG_DIR="/data/logs/$KWS_SERVICE_NAME"

numa='numactl --interleave all'
which cgstart > /dev/null 2>&1
if [[ $? != 0 ]]; then
  echo "start bookkeeper without cgstart"
  $numa bin/bookkeeper bookie
else
  echo "start bookkeeper with cgstart"
  $numa cgstart bin/bookkeeper bookie
fi
