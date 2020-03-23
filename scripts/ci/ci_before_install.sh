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
set -x

# shellcheck source=scripts/ci/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/_script_init.sh"

export UPGRADE_TO_LATEST_REQUIREMENTS="false"

# In case of CRON jobs on Travis we run builds without cache
if [[ "${TRAVIS_EVENT_TYPE:=}" == "cron" ]]; then
    echo
    echo "Disabling cache for CRON jobs"
    echo
    export DOCKER_CACHE="no-cache"
    export PULL_BASE_IMAGES="true"
    export UPGRADE_TO_LATEST_REQUIREMENTS="true"
fi

build_ci_image_on_ci

# We need newer version of six for Travis as they bundle 1.11.0 version
# Bowler is installed for backport packages build
pip install pre-commit bowler 'six~=1.14'
