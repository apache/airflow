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

echo
echo "Testing if all classes in import packages can be imported"
echo

OUT_FILE=$(mktemp)

if [[ ! ${INSTALL_AIRFLOW_VERSION:=""} =~ 1.10* ]]; then
    echo
    echo "ERROR! You can only install providers package in 1.10. airflow series."
    echo "You have: ${INSTALL_AIRFLOW_VERSION}"
    echo "Set INSTALL_AIRFLOW_VERSION variable to the version you want to install before running!"
    exit 1
else
    pushd /airflow_sources || exit
    echo
    echo "Installing remaining packages from 'all' extras"
    echo
    pip install ".[all]" >>"${OUT_FILE}" 2>&1
    echo
    echo "Uninstalling airflow after that"
    echo
    pip uninstall -y apache-airflow >>"${OUT_FILE}"  2>&1
    popd || exit
    echo
    echo "Install airflow from PyPI - ${INSTALL_AIRFLOW_VERSION}"
    echo
    pip install "apache-airflow==${INSTALL_AIRFLOW_VERSION}" >>"${OUT_FILE}" 2>&1
fi

echo
echo  Installing all packages at once in Airflow 1.10
echo

# Install all packages at once
pip install /dist/apache_airflow_backport_providers_*.whl >>"${OUT_FILE}" 2>&1


echo > "${OUT_FILE}"

echo
echo  Importing all classes in Airflow 1.10
echo

python3 /import_all_provider_classes.py
