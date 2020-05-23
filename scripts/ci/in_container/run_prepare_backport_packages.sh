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

LIST_OF_DIRS_FILE=$(mktemp)

# adding trap to exiting trap
HANDLERS="$( trap -p EXIT | cut -f2 -d \' )"
# shellcheck disable=SC2064
trap "${HANDLERS}${HANDLERS:+;}in_container_fix_ownership" EXIT

cd "${AIRFLOW_SOURCES}/airflow/providers" || exit 1

find . -type d | sed 's/.\///; s/\//\./g' | grep -E 'hooks|operators|sensors|secrets' \
    > "${LIST_OF_DIRS_FILE}"

cd "${AIRFLOW_SOURCES}/backport_packages" || exit 1

rm -rf dist/*
rm -rf -- *.egg-info

if [[ -z "$*" ]]; then
    PROVIDERS_PACKAGES=$(python3 setup_backport_packages.py list-providers-packages)

    PACKAGE_ERROR="false"
    # Check if all providers are included
    for PACKAGE in ${PROVIDERS_PACKAGES}
    do
        if ! grep -E "^${PACKAGE}" <"${LIST_OF_DIRS_FILE}" >/dev/null; then
            echo "The package ${PACKAGE} is not available in providers dir"
            PACKAGE_ERROR="true"
        fi
        sed -i "/^${PACKAGE}.*/d" "${LIST_OF_DIRS_FILE}"
    done

    if [[ ${PACKAGE_ERROR} == "true" ]]; then
        echo
        echo "ERROR! Some packages from backport_packages/setup_backport_packages.py are missing in providers dir"
        exit 1
    fi

    NUM_LINES=$(wc -l "${LIST_OF_DIRS_FILE}" | awk '{ print $1 }')
    if [[ ${NUM_LINES} != "0" ]]; then
        echo "ERROR! Some folders from providers package are not defined"
        echo "       Please add them to backport_packages/setup_backport_packages.py:"
        echo
        cat "${LIST_OF_DIRS_FILE}"
        echo
        exit 1
    fi
    BACKPORT_PACKAGES=$(python3 setup_backport_packages.py list-backportable-packages)
else
    if [[ "$1" == "--help" ]]; then
        echo
        echo "Builds all backport packages."
        echo
        echo "You can provide list of packages to build out of:"
        echo
        python3 setup_backport_packages.py list-providers-packages | tr '\n ' ' ' | fold -w 100 -s
        echo
        echo
        exit
    fi
    BACKPORT_PACKAGES="$*"
fi

echo "==================================================================================="
echo " Copying sources and doing refactor for backport packages"
echo "==================================================================================="
python3 setup_backport_packages.py prepare

VERSION_SUFFIX_FOR_PYPI=${VERSION_SUFFIX_FOR_PYPI:=""}
VERSION_SUFFIX_FOR_SVN=${VERSION_SUFFIX_FOR_SVN:=""}

echo "Version suffix for PyPI= ${VERSION_SUFFIX_FOR_PYPI}"
echo "Version suffix for SVN = ${VERSION_SUFFIX_FOR_SVN}"


for BACKPORT_PACKAGE in ${BACKPORT_PACKAGES}
do
    LOG_FILE=$(mktemp)
    echo "==================================================================================="
    echo " Preparing backport package ${BACKPORT_PACKAGE}"
    echo "-----------------------------------------------------------------------------------"
    python3 setup_backport_packages.py "${BACKPORT_PACKAGE}" clean --all >/dev/null 2>&1
    set +e
    python3 setup_backport_packages.py --version-suffix "${VERSION_SUFFIX_FOR_PYPI}" \
        "${BACKPORT_PACKAGE}" sdist bdist_wheel >"${LOG_FILE}" 2>&1
    RES="${?}"
    if [[ ${RES} != "0" ]]; then
        cat "${LOG_FILE}"
        exit "${RES}"
    fi
    set -e
    echo " Prepared backport package ${BACKPORT_PACKAGE}"
done

cd "${AIRFLOW_SOURCES}" || exit 1

pushd dist

if [[ ${VERSION_SUFFIX_FOR_SVN} != "" ]]; then
    for FILE in *.tar.gz
    do
        mv "${FILE}" "${FILE//\.tar\.gz/${VERSION_SUFFIX_FOR_SVN}-bin.tar.gz}"
    done
    for FILE in *.whl
    do
        mv "${FILE}" "${FILE//\-py3/${VERSION_SUFFIX_FOR_SVN}-py3}"
    done
fi

popd

AIRFLOW_PACKAGES_TGZ_FILE="/tmp/airflow-packages-$(date +"%Y%m%d-%H%M%S")-${VERSION_SUFFIX_FOR_SVN}${VERSION_SUFFIX_FOR_PYPI}.tar.gz"

tar -cvzf "${AIRFLOW_PACKAGES_TGZ_FILE}" dist/*.whl dist/*.tar.gz
echo
echo "Airflow packages are in dist folder and tar-gzipped in ${AIRFLOW_PACKAGES_TGZ_FILE}"
echo
if [[ "${CI:=false}" == "true" ]]; then
    echo
    echo "Sending all airflow packages to file.io"
    echo
    curl -F "file=@${AIRFLOW_PACKAGES_TGZ_FILE}" https://file.io
    echo
fi
