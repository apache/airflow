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

set -xeuo pipefail

MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# shellcheck source=scripts/ci/_utils.sh
.  "${MY_DIR}/_utils.sh"

if [[ ! -f "${MY_DIR}/runner-registration-token.yaml" ]]; then
    echo
    echo "You must get runner registration token from GitLab (Settings/CI/CD/Runners)"
    echo "And create '${MY_DIR}/runner-registration-token.yaml' file where you paste the token in"
    echo "Use '${MY_DIR}/runner-registration-token-template.yaml' as template"
    echo
    exit 1
fi

helm upgrade -f "${MY_DIR}/values.yaml" -f "${MY_DIR}/runner-registration-token.yaml" \
  "${RUNNER_NAME}" gitlab/gitlab-runner
