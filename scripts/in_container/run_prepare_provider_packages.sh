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

setup_backport_packages

LIST_OF_DIRS_FILE=$(mktemp)

cd "${AIRFLOW_SOURCES}/airflow/providers" || exit 1

find . -type d | sed 's/.\///; s/\//\./g' | grep -E 'hooks|operators|sensors|secrets' \
    > "${LIST_OF_DIRS_FILE}"

cd "${AIRFLOW_SOURCES}/provider_packages" || exit 1

rm -rf dist/*
rm -rf -- *.egg-info

if [[ -z "$*" ]]; then
    PROVIDERS_PACKAGES=$(python3 setup_provider_packages.py list-providers-packages)

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
        echo "ERROR! Some packages from provider_packages/setup_provider_packages.py are missing in providers dir"
        exit 1
    fi

    NUM_LINES=$(wc -l "${LIST_OF_DIRS_FILE}" | awk '{ print $1 }')
    if [[ ${NUM_LINES} != "0" ]]; then
        echo "ERROR! Some folders from providers package are not defined"
        echo "       Please add them to provider_packages/setup_provider_packages.py:"
        echo
        cat "${LIST_OF_DIRS_FILE}"
        echo
        exit 1
    fi
    PROVIDER_PACKAGES=$(python3 setup_provider_packages.py list-backportable-packages)
else
    if [[ "$1" == "--help" ]]; then
        echo
        echo "Builds all provider packages."
        echo
        echo "You can provide list of packages to build out of:"
        echo
        python3 setup_provider_packages.py list-providers-packages | tr '\n ' ' ' | fold -w 100 -s
        echo
        echo
        exit
    fi
    PROVIDER_PACKAGES="$*"
fi

if [[ ${BACKPORT_PACKAGES} == "true" ]]; then
    echo "==================================================================================="
    echo " Copying sources and refactoring code for backport provider packages"
    echo "==================================================================================="
else
    echo "==================================================================================="
    echo " Copying sources for provider packages"
    echo "==================================================================================="
fi
python3 refactor_provider_packages.py

VERSION_SUFFIX_FOR_PYPI=${VERSION_SUFFIX_FOR_PYPI:=""}
readonly VERSION_SUFFIX_FOR_PYPI

VERSION_SUFFIX_FOR_SVN=${VERSION_SUFFIX_FOR_SVN:=""}
readonly VERSION_SUFFIX_FOR_SVN

if [[ ${VERSION_SUFFIX_FOR_PYPI} != "" ]]; then
    echo
    echo "Version suffix for PyPI = ${VERSION_SUFFIX_FOR_PYPI}"
    echo
fi
if [[ ${VERSION_SUFFIX_FOR_SVN} != "" ]]; then
    echo
    echo "Version suffix for SVN  = ${VERSION_SUFFIX_FOR_SVN}"
    echo
fi

if [[ ${VERSION_SUFFIX_FOR_PYPI} != '' && ${VERSION_SUFFIX_FOR_SVN} != '' ]]; then
    if [[ ${VERSION_SUFFIX_FOR_PYPI} != "${VERSION_SUFFIX_FOR_SVN}" ]]; then
        >&2 echo
        >&2 echo "If you specify both version suffixes they must match"
        >&2 echo "However they are different: '${VERSION_SUFFIX_FOR_PYPI}' vs. '${VERSION_SUFFIX_FOR_SVN}'"
        >&2 echo
        exit 1
    else
        if [[ ${VERSION_SUFFIX_FOR_PYPI} =~ ^rc ]]; then
            >&2 echo
            >&2 echo "If you prepare an RC candidate, you need to specify only one of the PyPI/SVN suffixes"
            >&2 echo "However you specified both: '${VERSION_SUFFIX_FOR_PYPI}' vs. '${VERSION_SUFFIX_FOR_SVN}'"
            >&2 echo
            exit 2
        fi
        # Just use one of them - they are both the same:
        TARGET_VERSION_SUFFIX=${VERSION_SUFFIX_FOR_PYPI}
    fi
else
    if [[ ${VERSION_SUFFIX_FOR_PYPI} == '' && ${VERSION_SUFFIX_FOR_SVN} == '' ]]; then
        # Preparing "official version"
        TARGET_VERSION_SUFFIX=""
    else
        TARGET_VERSION_SUFFIX=${VERSION_SUFFIX_FOR_PYPI}${VERSION_SUFFIX_FOR_SVN}
        if [[ ! ${TARGET_VERSION_SUFFIX} =~ rc.* ]]; then
            >&2 echo
            >&2 echo "If you prepare an alpha/beta release, you need to specify both PyPI/SVN suffixes"
            >&2 echo "And they have to match. You specified only one."
            >&2 echo
            exit 2
        fi
    fi
fi

readonly TARGET_VERSION_SUFFIX

for PROVIDER_PACKAGE in ${PROVIDER_PACKAGES}
do
    LOG_FILE=$(mktemp)
    echo "==================================================================================="
    echo " Preparing ${PACKAGE_TYPE} package ${PROVIDER_PACKAGE} "
    if [[ "${VERSION_SUFFIX_FOR_PYPI}" == '' && "${VERSION_SUFFIX_FOR_SVN}" == '' ]]; then
        echo
        echo " Official version"
        echo
    elif [[ "${VERSION_SUFFIX_FOR_PYPI}" == '' ]]; then
        echo
        echo " Package Version for SVN release candidate: ${TARGET_VERSION_SUFFIX}"
        echo
    elif [[ "${VERSION_SUFFIX_FOR_SVN}" == '' ]]; then
        echo
        echo " Package Version for PyPI release candidate: ${TARGET_VERSION_SUFFIX}"
        echo
    else
        # Both SV/PYPI are set to the same version here!
        echo
        echo " Pre-release version: ${TARGET_VERSION_SUFFIX}"
        echo
    fi
    echo "-----------------------------------------------------------------------------------"
    set +e
    python3 setup_provider_packages.py "${PROVIDER_PACKAGE}" clean --all >"${LOG_FILE}" 2>&1
    RES="${?}"
    if [[ ${RES} != "0" ]]; then
        cat "${LOG_FILE}"
        exit "${RES}"
    fi
    echo > "${LOG_FILE}"

    PACKAGE_DIR=${PROVIDER_PACKAGE//./\/}

    PATTERN="airflow\/providers\/(.*)\/${PACKAGE_PREFIX_UPPERCASE}PROVIDERS_CHANGES_.*.md"
    CHANGELOG_FILE="CHANGELOG.txt"

    echo > "${CHANGELOG_FILE}"
    CHANGES_FILES=$(find "airflow/providers/${PACKAGE_DIR}" \
        -name "${PACKAGE_PREFIX_UPPERCASE}PROVIDERS_CHANGES_*.md" | sort -r)
    LAST_PROVIDER_ID=""
    for FILE in ${CHANGES_FILES}
    do
        [[ ${FILE} =~ ${PATTERN} ]]
        PROVIDER_ID=${BASH_REMATCH[1]//\//.}
        {
            if [[ ${LAST_PROVIDER_ID} != "${PROVIDER_ID}" ]]; then
                echo
                echo "Provider: ${BASH_REMATCH[1]//\//.}"
                echo
                LAST_PROVIDER_ID=${PROVIDER_ID}
            else
                echo
            fi
            cat "${FILE}"
            echo
        } >> "${CHANGELOG_FILE}"
    done

    echo "Changelog prepared in ${CHANGELOG_FILE} for ${PACKAGE_DIR}"
    set +e
    python3 setup_provider_packages.py "${PROVIDER_PACKAGE}" clean --all >"${LOG_FILE}" 2>&1
    RES="${?}"
    if [[ ${RES} != "0" ]]; then
        cat "${LOG_FILE}"
        exit "${RES}"
    fi
    python3 setup_provider_packages.py --version-suffix "${VERSION_SUFFIX_FOR_PYPI}" \
        "${PROVIDER_PACKAGE}" sdist bdist_wheel >"${LOG_FILE}" 2>&1
    RES="${?}"
    if [[ ${RES} != "0" ]]; then
        cat "${LOG_FILE}"
        exit "${RES}"
    fi
    set -e
    echo " Prepared ${PACKAGE_TYPE} package ${PROVIDER_PACKAGE}"
    echo "==================================================================================="
done

cd "${AIRFLOW_SOURCES}" || exit 1

pushd dist

if [[ ${VERSION_SUFFIX_FOR_PYPI} != "${TARGET_VERSION_SUFFIX}" ]]; then
    # In case we prepare different suffix for SVN and PYPI, rename the generated files
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

AIRFLOW_PACKAGES_TGZ_FILE="/files/airflow-packages-$(date +"%Y%m%d-%H%M%S")-${TARGET_VERSION_SUFFIX}.tar.gz"

tar -cvzf "${AIRFLOW_PACKAGES_TGZ_FILE}" dist/*.whl dist/*.tar.gz
echo
echo "Airflow packages are in dist folder and tar-gzipped in ${AIRFLOW_PACKAGES_TGZ_FILE}"
echo
