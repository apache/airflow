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
# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"
set -euo pipefail

export PYTHON_MAJOR_MINOR_VERSION=${PYTHON_MAJOR_MINOR_VERSION:="3.6"}
export KIND_CLUSTER_NAME=${KIND_CLUSTER_NAME:="airflow-python-${PYTHON_MAJOR_MINOR_VERSION}-${KUBERNETES_VERSION}"}
export KUBERNETES_MODE=${KUBERNETES_MODE:="image"}

# adding trap to exiting trap
HANDLERS="$( trap -p EXIT | cut -f2 -d \' )"
# shellcheck disable=SC2064
trap "${HANDLERS}${HANDLERS:+;}dump_kind_logs" EXIT

get_environment_for_builds_on_ci
initialize_kind_variables
make_sure_kubernetes_tools_are_installed
prepare_prod_build
build_prod_images
build_image_for_kubernetes_tests
load_image_to_kind_cluster
deploy_airflow_with_helm
forward_port_to_kind_webserver
deploy_test_kubernetes_resources
