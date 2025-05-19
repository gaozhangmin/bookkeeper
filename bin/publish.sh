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
set -ex
# Time-stamp: <2023-07-03 17:54:38 Monday by ahei>

# @file publish.sh
# @version 1.0
# @author ahei

readonly PROGRAM_NAME="publish.sh"
readonly PROGRAM_VERSION="1.0.0"

# 编译
mvn clean install -DskipTests -T 8

echo "upload ..."
echo "上传到【线上】产品库"
curl "http://ksp.corp.kuaishou.com/api/product/products/bookkeeper/versions/upload/" -F file=@bookkeeper-dist/server/target/bookkeeper-server-bin.tar.gz -F region="cn,br,us" -H 'host: ksp.corp.kuaishou.com' -F msg="$publishLog"