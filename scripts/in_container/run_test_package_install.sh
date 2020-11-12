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
OUT_FILE_PRINTED_ON_ERROR=$(mktemp)

setup_provider_packages

echo
echo "Testing if all provider packages can be installed separately on Airflow and cause no side effects"
echo

if [[ ${INSTALL_AIRFLOW_VERSION:=""} == "wheel" ]]; then
    echo
    echo "Installing the airflow prepared from wheels"
    echo
    pip install /dist/apache_airflow-*.whl
    # Need to add excluded apache beam
    pip install apache-beam[gcp]
    echo
elif [[ ! ${INSTALL_AIRFLOW_VERSION} =~ ^1\.10\..* ]]; then
    >&2 echo
    >&2 echo "ERROR! You should install providers package in 1.10. Airflow series."
    >&2 echo "You have: ${INSTALL_AIRFLOW_VERSION}"
    >&2 echo "Set INSTALL_AIRFLOW_VERSION variable to the version you want to install before running!"
    exit 1
else
    # install specified airflow from PyPI
    pip install "apache-airflow==${INSTALL_AIRFLOW_VERSION}" >>"${OUT_FILE_PRINTED_ON_ERROR}" 2>&1
fi

EXTRA_FLAGS=""

if [[ ${BACKPORT_PACKAGES} != "true" ]]; then
    # Install providers without deps as we do not have yet airflow 2.0 released
    EXTRA_FLAGS="--no-deps"
fi

# Install all packages at once
if [[ ${1} == "whl" ]]; then
    echo
    echo "Installing all whl packages"
    echo
    pip install ${EXTRA_FLAGS} "/dist/apache_airflow_${PACKAGE_PREFIX_LOWERCASE}providers_"*.whl
elif [[ ${1} == "tar.gz" ]]; then
    echo
    echo "Installing all tar.gz packages"
    echo
    pip install ${EXTRA_FLAGS} "/dist/apache-airflow-${PACKAGE_PREFIX_HYPHEN}providers-"*.tar.gz
else
    >&2 echo
    >&2 echo "ERROR! Wrong package type ${1}. Should be whl or tar.gz"
    >&2 echo
    exit 1
fi

echo "==================================================================================="
