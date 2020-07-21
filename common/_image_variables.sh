#!/usr/bin/env bash
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

# shellcheck source=common/_default_branch.sh
. "${AIRFLOW_SOURCES}/common/_default_branch.sh"

# You can override DOCKERHUB_USER to use your own DockerHub account and play with your
# own docker images. In this case you can build images locally and push them
export DOCKERHUB_USER=${DOCKERHUB_USER:="apache"}

# You can override DOCKERHUB_REPO to use your own DockerHub repository and play with your
# own docker images. In this case you can build images locally and push them
export DOCKERHUB_REPO=${DOCKERHUB_REPO:="airflow"}

# read branch name from what has been set from sources (It can also be overridden)
export BRANCH_NAME=${BRANCH_NAME:=${DEFAULT_BRANCH}}
