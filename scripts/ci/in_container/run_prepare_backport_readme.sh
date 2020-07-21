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
# shellcheck source=scripts/ci/in_container/_in_container_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/_in_container_script_init.sh"

OUT_FILE=$(mktemp)

# adding trap to exiting trap
HANDLERS="$( trap -p EXIT | cut -f2 -d \' )"
# shellcheck disable=SC2064
trap "${HANDLERS}${HANDLERS:+;}in_container_fix_ownership" EXIT

cd "${AIRFLOW_SOURCES}" || exit 1

# install extra packages missing in devel_ci
export PYTHONPATH="${AIRFLOW_SOURCES}"

echo
echo "Installing remaining packages from 'all' extras"
echo
pip install -e ".[all]" >>"${OUT_FILE}" 2>&1

echo > "${OUT_FILE}"

cd "${AIRFLOW_SOURCES}/backport_packages" || exit 1

python3 setup_backport_packages.py update-package-release-notes "$@"

AIRFLOW_BACKPORT_README_TGZ_FILE="/dist/airflow-backport-readme-$(date +"%Y-%m-%d-%H.%M.%S").tar.gz"

cd "${AIRFLOW_SOURCES}" || exit 1

find airflow/providers \( -name 'README.md' -o -name 'PROVIDERS_CHANGES*' \) -print0 | \
    tar --null --no-recursion -cvzf "${AIRFLOW_BACKPORT_README_TGZ_FILE}" -T -
echo
echo "Airflow readme for backport packages are tar-gzipped in ${AIRFLOW_BACKPORT_README_TGZ_FILE}"
echo
if [[ "${CI:=false}" == "true" ]]; then
    echo
    echo "Sending all airflow packages to file.io"
    echo
    curl -F "file=@${AIRFLOW_PACKAGES_TGZ_FILE}" https://file.io
    echo
fi
