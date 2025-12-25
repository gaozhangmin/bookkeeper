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

export JAVA_HOME=${JAVA_HOME:-"/opt/kbox/latest/jdk/snowman17"}

mkdir -p /home/web_server/bookkeeper

ln -sfn $JAVA_HOME /home/web_server/bookkeeper/jdk-17

# install jdk
bk_basic=/home/web_server/kuaishou-runner/bk-autorecovery
if [ ! -d "$bk_basic" ]; then
  mkdir -p "$bk_basic"
fi

## save ENV vars
echo "export KWS_SERVICE_NAME=$KWS_SERVICE_NAME" > $bk_basic/conf/kwai_runtime.sh

## set directMem
directMem="5g"
# set heap size
heapSize="5g"
if [[ $KWS_SERVICE_NAME == *kop* ]]; then
  directMem="5g"
  heapSize="5g"
  # install python kconf
  pip3 install infra-kconf
  # generate cluster config
  python3 $bk_basic/bin/init_cluster_config.py
fi

export BOOKIE_MEM_OPTS="-Xms${heapSize} -Xmx${heapSize} -XX:MaxDirectMemorySize=${directMem} -XX:-UseNUMA"
export BOOKIE_ROOT_LOG_APPENDER="ROLLINGFILE"
export BOOKIE_LOG_DIR=/home/web_server/kuaishou-runner/log
export BOOKIE_LOG_FILE=bk-autorecovery.log


# 启动 Java 进程并获取其 PID
$bk_basic/bin/bookkeeper autorecovery &
JAVA_PID=$!

# 等待进程启动
sleep 5

# 获取 PID
JAVA_PID=$(jps | grep AutoRecoveryMain | awk '{print $1}')

echo "[SERVICE_START_UP] pid=$JAVA_PID,status=success,start_time=$(date +%s%3N)" > /home/web_server/kuaishou-runner/log/startup.log

# 定义一个函数来处理 SIGTERM 信号
handle_sigterm() {
    echo "Received SIGTERM, forwarding to Java process..."
    kill -TERM "$JAVA_PID"
    while kill -0 $JAVA_PID 2>/dev/null; do
        sleep 1
        echo "Waiting for Java process to terminate..."
    done
}

# 捕获 SIGTERM 信号并调用 handle_sigterm 函数
trap 'handle_sigterm' TERM

# 等待 Java 进程结束
wait "$JAVA_PID"

echo "Java process has terminated."

