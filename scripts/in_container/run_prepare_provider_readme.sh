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
. "$( dirname "${BASH_SOURCE[0]}" )/_in_container_script_init.sh"

setup_provider_packages

cd "${AIRFLOW_SOURCES}" || exit 1

# install extra packages missing in devel_ci
export PYTHONPATH="${AIRFLOW_SOURCES}"

verify_suffix_versions_for_package_preparation

pip install --upgrade "pip==${AIRFLOW_PIP_VERSION}"

# TODO: remove it when devel_all == devel_ci
echo
echo "Installing remaining packages from 'all' extras"
echo
pip install ".[devel_all]"

cd "${AIRFLOW_SOURCES}/provider_packages" || exit 1

python3 "${AIRFLOW_SOURCES}/dev/provider_packages/prepare_provider_packages.py" \
    --version-suffix "${TARGET_VERSION_SUFFIX}" \
    update-package-release-notes "$@"

AIRFLOW_PROVIDER_README_TGZ_FILE="/files/airflow-readme-$(date +"%Y-%m-%d-%H.%M.%S").tar.gz"

cd "${AIRFLOW_SOURCES}" || exit 1

find airflow/providers \( \
        -name "${PACKAGE_PREFIX_UPPERCASE}PROVIDERS_CHANGES*" \
        -o -name "${PACKAGE_PREFIX_UPPERCASE}README.md" \
        -o -name "${PACKAGE_PREFIX_UPPERCASE}setup.py" \
        -o -name "${PACKAGE_PREFIX_UPPERCASE}setup.cfg" \
        \) \
        -print0 | \
    tar --null --no-recursion -cvzf "${AIRFLOW_PROVIDER_README_TGZ_FILE}" -T -
echo
echo "Airflow readme for ${PACKAGE_TYPE} provider packages format ${PACKAGE_FORMAT} are tar-gzipped in ${AIRFLOW_PROVIDER_README_TGZ_FILE}"
echo
