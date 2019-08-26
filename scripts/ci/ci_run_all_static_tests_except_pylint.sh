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

set -euo pipefail

MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export AIRFLOW_CI_SILENT=${AIRFLOW_CI_SILENT:="true"}
export ASSUME_QUIT_TO_ALL_QUESTIONS=${ASSUME_QUIT_TO_ALL_QUESTIONS:="true"}

# shellcheck source=scripts/ci/_utils.sh
. "${MY_DIR}/_utils.sh"

basic_sanity_checks

force_python_3_5

script_start

rebuild_image_if_needed_for_static_checks

rebuild_image_if_needed_for_checklicence

SKIP=pylint pre-commit run --all-files --show-diff-on-failure

script_end
