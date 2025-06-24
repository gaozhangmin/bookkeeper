#!/usr/bin/env bash

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

set -e -x -u

BINDIR=`dirname "$0"`
BK_HOME=`cd $BINDIR/../..;pwd`

cd $BK_HOME

project_version=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
echo "project_version: $project_version"

url='unknown'
repositoryId='unknown'
if [[ "$project_version" == *"-SNAPSHOT" ]]; then
  url='http://nexus.corp.kuaishou.com:88/nexus/content/repositories/snapshots/'
  repositoryId='kuaishou.snapshots'
else
  url='http://nexus.corp.kuaishou.com:88/nexus/content/repositories/releases/'
  repositoryId='kuaishou.releases'
fi

function deploy_nar_jar() {
  module=$1
  echo "deploy jar for $module"

  mvn deploy:deploy-file \
    -Dfile="$module/target/$module-$project_version.jar" \
    -Durl="$url" \
    -DrepositoryId="$repositoryId" \
    -DgroupId=org.apache.bookkeeper \
    -DartifactId="$module" \
    -Dversion="$project_version" \
    -Dpackaging=jar \
    -DgeneratePom=false \
    -DpomFile="$module/pom.xml"

  echo "deploy $module success!!"
}

deploy_nar_jar circe-checksum
deploy_nar_jar native-io
deploy_nar_jar cpu-affinity

