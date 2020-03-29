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
# shellcheck source=scripts/ci/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/_script_init.sh"

set -euo pipefail

export KUBERNETES_VERSION=${KUBERNETES_VERSION:="v1.15.3"}
export PYTHON_MAJOR_MINOR_VERSION=${PYTHON_MAJOR_MINOR_VERSION:="3.6"}
export KIND_CLUSTER_NAME=${KIND_CLUSTER_NAME:="airflow-python-${PYTHON_MAJOR_MINOR_VERSION}-${KUBERNETES_VERSION}"}
export KUBERNETES_MODE=${KUBERNETES_MODE:="image"}

get_ci_environment
check_kind_and_kubectl_are_installed
build_kubernetes_image
load_image_to_kind_cluster
prepare_kubernetes_app_variables
prepare_kubernetes_resources
apply_kubernetes_resources
wait_for_airflow_pods_up_and_running
wait_for_airflow_webserver_up_and_running
