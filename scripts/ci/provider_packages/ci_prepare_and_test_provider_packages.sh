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


"${SCRIPTS_CI_DIR}/provider_packages/ci_prepare_provider_readme.sh"
"${SCRIPTS_CI_DIR}/provider_packages/ci_prepare_provider_packages.sh"

if [[ ${BACKPORT_PACKAGES} != "true" ]]; then
    # Prepare airflow's wheel
    python setup.py compile_assets sdist bdist_wheel
    rm -rf -- *egg-info*
fi

"${SCRIPTS_CI_DIR}/provider_packages/ci_test_provider_packages_install_separately.sh"
"${SCRIPTS_CI_DIR}/provider_packages/ci_test_provider_packages_import_all_classes.sh"

dump_file="/tmp/airflow_provider_packages_$(date +"%Y%m%d-%H%M%S").tar.gz"

cd "${AIRFLOW_SOURCES}/dist" || exit 1
tar -cvzf "${dump_file}" .

echo "Packages are in dist and also tar-gzipped in ${dump_file}"
