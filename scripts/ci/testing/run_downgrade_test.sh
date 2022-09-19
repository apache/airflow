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

testing::get_docker_compose_local
testing::setup_docker_compose_backend "downgrade"
# This runs downgrades for DBs created from the migration file
testing::run_command_in_docker "downgrade" "airflow db reset --skip-init -y \
    && airflow db upgrade --to-revision heads && airflow db downgrade -r e959f08ac86c -y \
    && airflow db upgrade"
# This tests upgrade/downgrade for DBs created from the ORM
testing::run_command_in_docker "downgrade" "airflow db reset -y \
    && airflow db upgrade && airflow db downgrade -r e959f08ac86c -y \
    && airflow db upgrade"
