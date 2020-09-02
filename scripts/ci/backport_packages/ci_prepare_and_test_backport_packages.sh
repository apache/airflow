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
export PYTHON_MAJOR_MINOR_VERSION=${PYTHON_MAJOR_MINOR_VERSION:-3.6}

# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

"${SCRIPTS_CI_DIR}/backport_packages/ci_prepare_backport_readme.sh"
"${SCRIPTS_CI_DIR}/backport_packages/ci_prepare_backport_packages.sh"
"${SCRIPTS_CI_DIR}/backport_packages/ci_test_backport_packages_install_separately.sh"
"${SCRIPTS_CI_DIR}/backport_packages/ci_test_backport_packages_import_all_classes.sh"

DUMP_FILE="/tmp/airflow_provider_packages_$(date +"%Y%m%d-%H%M%S").tar.gz"

cd "${AIRFLOW_SOURCES}/dist" || exit 1
tar -cvzf "${DUMP_FILE}" .

echo "Packages are in dist and also tar-gzipped in ${DUMP_FILE}"
