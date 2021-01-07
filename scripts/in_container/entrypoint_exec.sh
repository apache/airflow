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
# shellcheck source=scripts/in_container/_in_container_script_init.sh
. /opt/airflow/scripts/in_container/_in_container_script_init.sh

# shellcheck source=scripts/in_container/configure_environment.sh
. "$( dirname "${BASH_SOURCE[0]}" )/configure_environment.sh"

# shellcheck source=scripts/in_container/run_init_script.sh
. "$( dirname "${BASH_SOURCE[0]}" )/run_init_script.sh"

# shellcheck source=scripts/in_container/run_tmux.sh
. "$( dirname "${BASH_SOURCE[0]}" )/run_tmux.sh"

exec /bin/bash "${@}"
