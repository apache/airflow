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

export RUNNER_NAME="gitlab-runner-apache"
export CLUSTER_NAME="apache-airflow"
export PROJECT_ID="apache-airflow-testing"
export ZONE="europe-west1-b"
export CLUSTER_VERSION="1.13.7-gke.8"
export MAX_PODS_PER_NODE="110"
export MIN_NODES="${MIN_NODES}"
export MAX_NODES="12"
