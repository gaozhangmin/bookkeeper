#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# this is the top level Lombok configuration file
# see https://projectlombok.org/features/configuration for reference

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# this is the top level Lombok configuration file
# see https://projectlombok.org/features/configuration for reference

workspace=$(cd $(dirname $0) && pwd -P)
cd $workspace

export MAVEN_HOME=/home/scmtools/thirdparty/maven-3.6.3
export JAVA_HOME=/usr/local/jdk-11.0.2
export PATH=$MAVEN_HOME/bin:$JAVA_HOME/bin:$PATH

## function
function build() {
    mvn clean install  -DskipTests -Drat.skip=true

}

function make_output() {
    # 新建output目录
    local output="./output"
    rm -rf $output &>/dev/null
    mkdir -p $output &>/dev/null

    # 填充output目录, output内的内容 即为 线上部署内容
    (
        cp -ap bookkeeper-dist/server/target/bookkeeper-server-*-bin.tar.gz $output
        echo -e "make output ok."
    ) || { echo -e "make output error"; exit 2; } # 填充output目录失败后, 退出码为 非0
}

## internals
function gitversion() {
    git log -1 --pretty=%h > $gitversion
    local gv=`cat $gitversion`
    echo "$gv"
}


##########################################
## main
## 其中,
##      1.进行编译
##      2.生成部署包output
##########################################

# 1.进行编译
build

# 2.生成部署包output
make_output

# 编译成功
echo -e "build done"
exit 0
