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
LIBRARIES_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../libraries/" && pwd)
# shellcheck source=scripts/ci/libraries/_all_libs.sh
source "${LIBRARIES_DIR}/_all_libs.sh"

export SEMAPHORE_NAME="kubernetes-tests-upgrade"

initialization::set_output_color_variables
initialization::initialize_image_build_variables

parallel::make_sure_gnu_parallel_is_installed
parallel::make_sure_python_versions_are_specified
parallel::make_sure_kubernetes_versions_are_specified

parallel::get_maximum_parallel_k8s_jobs
parallel::run_helm_tests_in_parallel \
    "$(dirname "${BASH_SOURCE[0]}")/ci_upgrade_cluster_with_different_executors_single_job.sh" "${@}"

# this will exit with error code in case some of the tests failed
parallel::print_job_summary_and_return_status_code
