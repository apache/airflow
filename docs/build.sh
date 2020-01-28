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

set -euo pipefail

MY_DIR="$(cd "$(dirname "$0")" && pwd)"
pushd "${MY_DIR}" &>/dev/null || exit 1

echo
echo "Working in ${MY_DIR} folder"
echo


if [[ -f /.dockerenv ]]; then
    # This script can be run both - in container and outside of it.
    # Here we are inside the container which means that we should (when the host is Linux)
    # fix permissions of the _build and _api folders via sudo.
    # Those files are mounted from the host via docs folder and we might not have permissions to
    # write to those directories (and remove the _api folder).
    # We know we have sudo capabilities inside the container.
    echo "Creating the _build and _api folders in case they do not exist"
    sudo mkdir -pv _build
    sudo mkdir -pv _api
    echo "Created the _build and _api folders in case they do not exist"
else
    # We are outside the container so we simply make sure that the directories exist
    echo "Creating the _build and _api folders in case they do not exist"
    mkdir -pv _build
    mkdir -pv _api
    echo "Creating the _build and _api folders in case they do not exist"
fi

echo "Removing content of the _build and _api folders"
rm -rf _build/*
rm -rf _api/*
echo "Removed content of the _build and _api folders"


set +e
# shellcheck disable=SC2063
NUM_INCORRECT_USE_LITERALINCLUDE=$(grep -inR --include \*.rst 'literalinclude::.\+example_dags' . | \
    tee /dev/tty |
    wc -l |\
    tr -d '[:space:]')
set -e

echo
echo "Checking for presence of literalinclude in example DAGs"
echo

if [[ "${NUM_INCORRECT_USE_LITERALINCLUDE}" -ne "0" ]]; then
    echo
    echo "Unexpected problems found in the documentation. "
    echo "You should use a exampleinclude directive to include example DAGs."
    echo "Currently, ${NUM_INCORRECT_USE_LITERALINCLUDE} problem found."
    echo
    exit 1
else
    echo
    echo "No literalincludes in example DAGs found"
    echo
fi

echo
echo "Checking the status of the operators-and-hooks-ref.rst file."
echo

mapfile -t DEPRECATED_MODULES < <(grep -R -i -l 'This module is deprecated.' ../airflow --include '*.py' | \
    cut -d "/" -f 2- | \
    sort | \
    uniq | \
    cut -d "." -f 1 | \
    sed "s#/#.#g")

IGNORED_MISSING_MODULES=('airflow.gcp.hooks.base')

mapfile -t ALL_MODULES < <(find ../airflow/{,gcp/,providers/*/*/,providers/*/}{operators,sensors,hooks} -name "*.py" | \
    grep -v "__init__" | \
    grep -v "__pycache__" | \
    cut -d "/" -f 2- | \
    cut -d "." -f 1 | \
    sed "s#/#.#g" | \
    sort | \
    uniq | \
    grep -vf <(printf '%s\n' "${DEPRECATED_MODULES[@]}") |\
    grep -vf <(printf '%s\n' "${IGNORED_MISSING_MODULES[@]}"))

# shellcheck disable=SC2002
mapfile -t CURRENT_MODULES < <(cat operators-and-hooks-ref.rst | \
    grep ":mod:" | \
    cut -d '`' -f 2 | \
    sort | \
    uniq | \
    grep -v "__pycache__")

mapfile -t MISSING_MODULES < \
    <(\
        comm -2 -3 \
        <(printf '%s\n' "${ALL_MODULES[@]}" | sort ) \
        <(printf '%s\n' "${CURRENT_MODULES[@]}" | sort)
    )

if [[ "${#MISSING_MODULES[@]}" -ne "0" ]]; then
    echo
    echo "Unexpected problems found in the documentation."
    echo "You should try to keep the list of operators and hooks up to date."
    echo
    echo "Missing modules:"
    printf '%s\n' "${MISSING_MODULES[@]}"
    echo
    echo "Please add this module to operators-and-hooks-ref.rst file."
    echo
    exit 1
else
    echo
    echo "The operators-and-hooks-ref.rst file seems to be in good condition."
    echo
fi


SUCCEED_LINE=$(make html |\
    tee /dev/tty |\
    grep 'build succeeded' |\
    head -1)

NUM_CURRENT_WARNINGS=$(echo "${SUCCEED_LINE}" |\
    sed -E 's/build succeeded, ([0-9]+) warnings?\./\1/g')

if [[  -f /.dockerenv ]]; then
    # We are inside the container which means that we should fix back the permissions of the
    # _build and _api folder files, so that they can be accessed by the host user
    # The _api folder should be deleted by then but just in case we should change the ownership
    echo "Changing ownership of docs/_build folder back to ${HOST_USER_ID}:${HOST_GROUP_ID}"
    sudo chown "${HOST_USER_ID}":"${HOST_GROUP_ID}" _build
    if [[ -d _api ]]; then
        sudo chown "${HOST_USER_ID}":"${HOST_GROUP_ID}" _api
    fi
    echo "Changed ownership of docs/_build folder back to ${HOST_USER_ID}:${HOST_GROUP_ID}"
fi


if echo "${SUCCEED_LINE}" | grep -q "warning"; then
    echo
    echo "Unexpected problems found in the documentation. "
    echo "Currently, ${NUM_CURRENT_WARNINGS} warnings found. "
    echo
    exit 1
fi

popd &>/dev/null || exit 1
