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

#
# Fixes ownership for files created inside container (files owned by root will be owned by host user)
#
# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

docker-compose \
    -f "${SCRIPTS_CI_DIR}/docker-compose/base.yml" \
    -f "${SCRIPTS_CI_DIR}/docker-compose/local.yml" \
    -f "${SCRIPTS_CI_DIR}/docker-compose/files.yml" \
    -f "${SCRIPTS_CI_DIR}/docker-compose/forward-credentials.yml" \
    run --entrypoint /bin/bash \
    airflow -c /opt/airflow/scripts/in_container/run_fix_ownership.sh
