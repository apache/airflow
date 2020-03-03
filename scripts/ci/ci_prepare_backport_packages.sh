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
export PYTHON_VERSION=${PYTHON_VERSION:-3.6}

# shellcheck source=scripts/ci/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/_script_init.sh"

cd "${MY_DIR}/../../backport_packages" || exit 1

rm -rf dist/*
rm -rf -- *.egg-info

BACKPORT_PACKAGES=$(python3 setup_backport_packages.py list-backport-packages)

echo "-----------------------------------------------------------------------------------"
echo " Copying sources and doing refactor for backporting"
echo "-----------------------------------------------------------------------------------"
echo
python3 setup_backport_packages.py prepare

for BACKPORT_PACKAGE in ${BACKPORT_PACKAGES}
do
    echo
    echo "-----------------------------------------------------------------------------------"
    echo " Preparing backporting package ${BACKPORT_PACKAGE}"
    echo "-----------------------------------------------------------------------------------"
    echo
    python3 setup_backport_packages.py "${BACKPORT_PACKAGE}" clean --all
    python3 setup_backport_packages.py "${BACKPORT_PACKAGE}" sdist bdist_wheel >/dev/null
done

echo
echo "-----------------------------------------------------------------------------------"
echo " Preparing backporting package providers (everything)"
echo "-----------------------------------------------------------------------------------"
echo
python3 setup_backport_packages.py providers clean --all
python3 setup_backport_packages.py providers sdist bdist_wheel >/dev/null

DUMP_FILE="/tmp/airflow_provider_packages_$(date +"%Y%m%d-%H%M%S").tar.gz"

tar -cvzf "${DUMP_FILE}" "dist"

echo "Packages are prepared in ${DUMP_FILE}"

if [[ "${CI:=false}" == "true" ]]; then
    curl -F "file=@${DUMP_FILE}" https://file.io
fi
