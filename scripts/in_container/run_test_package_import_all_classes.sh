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

echo
echo "Testing if all classes in import packages can be imported"
echo

OUT_FILE_PRINTED_ON_ERROR=$(mktemp)

if [[ ${INSTALL_AIRFLOW_VERSION:=""} == "wheel"  ]]; then
    echo
    echo "Installing the airflow prepared from wheels"
    echo
    pip uninstall -y apache-airflow
    pip install /dist/apache_airflow-*.whl
    # Need to add excluded apache. All the rest should be installed by extras
    pip install apache-beam[gcp]
    echo
elif [[ ! ${INSTALL_AIRFLOW_VERSION:=""} =~ ^1\.10\..* ]]; then
    echo
    echo "ERROR! You can only install providers package in 1.10. airflow series."
    echo "You have: ${INSTALL_AIRFLOW_VERSION}"
    echo "Set INSTALL_AIRFLOW_VERSION variable to the version you want to install before running!"
    exit 1
else
    pushd /airflow_sources > /dev/null || exit
    echo
    echo "Installing remaining packages from 'all' extras"
    echo
    pip install ".[all]" >>"${OUT_FILE_PRINTED_ON_ERROR}" 2>&1
    echo
    echo "Uninstalling airflow after that"
    echo
    pip uninstall -y apache-airflow >>"${OUT_FILE_PRINTED_ON_ERROR}"  2>&1
    popd >/dev/null || exit
    echo
    echo "Install airflow from PyPI - ${INSTALL_AIRFLOW_VERSION}"
    echo
    pip install "apache-airflow==${INSTALL_AIRFLOW_VERSION}" >>"${OUT_FILE_PRINTED_ON_ERROR}" 2>&1
fi

echo
echo  "Installing all packages at once for Airflow ${INSTALL_AIRFLOW_VERSION}"
echo

EXTRA_FLAGS=""

if [[ ${BACKPORT_PACKAGES} != "true" ]]; then
    # Install providers without deps as we do not have yet airflow 2.0 released
    EXTRA_FLAGS="--no-deps"
fi

# Install all packages at once
pip install ${EXTRA_FLAGS} /dist/apache_airflow*providers_*.whl >>"${OUT_FILE_PRINTED_ON_ERROR}" 2>&1

echo > "${OUT_FILE_PRINTED_ON_ERROR}"

echo
echo  Importing all classes in Airflow 1.10
echo

# We have to move to a directory where "airflow
unset PYTHONPATH
# We need to make sure we are not in the airflow checkout, otherwise it will automatically be added to the
# import path
cd /

declare -a IMPORT_CLASS_PARAMETERS

PROVIDER_PATHS=$(python3 <<EOF 2>/dev/null
import airflow.providers;
path=airflow.providers.__path__
for p in path._path:
    print(p)
EOF
)
export PROVIDER_PATHS

echo "Searching for providers packages in:"
echo "${PROVIDER_PATHS}"


while read -r provider_path
do
    IMPORT_CLASS_PARAMETERS+=("--path" "${provider_path}")
done < <(echo "${PROVIDER_PATHS}")

python3 /opt/airflow/dev/import_all_classes.py "${IMPORT_CLASS_PARAMETERS[@]}"
