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


function redraw_progress_bar { # int barsize, int base, int current, int top
    # Source: https://stackoverflow.com/a/20311674
    local barsize=$1
    local base=$2
    local current=$3
    local top=$4
    local j=0
    local progress=$(( (barsize * (current - base)) / (top - base ) ))
    echo -n "["
    for ((j=0; j < progress; j++)) ; do echo -n '='; done
    echo -n '=>'
    for ((j=progress; j < barsize ; j++)) ; do echo -n ' '; done
    echo -n "] $current / $top " $'\r'
}

if ! command -v lynx; then
    echo "This script requires lynx to work properly."
    echo
    echo "For futher informatiion, look at: http://lynx.browser.org/"
    exit
fi

MY_DIR="$(cd "$(dirname "$0")" && pwd)"
pushd "${MY_DIR}" &>/dev/null || exit 1

echo
echo "Working in ${MY_DIR} folder"
echo

if [[ ! -f _build/html/index.html ]]; then
   echo "You should build documentation first."
   echo
   echo "If you use the breeze environment then you can do it using the following command:"
   echo "./breeze --build-docs"
   echo
   exit 1
fi

if [[ -f /.dockerenv ]]; then
    # This script can be run both - in container and outside of it.
    # Here we are inside the container which means that we should (when the host is Linux)
    # fix permissions of the _build and _api folders via sudo.
    # Those files are mounted from the host via docs folder and we might not have permissions to
    # write to those directories (and remove the _api folder).
    # We know we have sudo capabilities inside the container.
    echo "Creating the _build and _api folders in case they do not exist"
    sudo mkdir -pv _build/html
    sudo mkdir -pv _api
    echo "Created the _build and _api folders in case they do not exist"
    echo "Changing ownership of _build and _api folders to ${AIRFLOW_USER}:${AIRFLOW_USER}"
    sudo chown -R "${AIRFLOW_USER}":"${AIRFLOW_USER}" .
    echo "Changed ownership of the whole doc folder to ${AIRFLOW_USER}:${AIRFLOW_USER}"
else
    # We are outside the container so we simply make sure that the directories exist
    echo "Creating the _build and _api folders in case they do not exist"
    mkdir -pv _build/html
    echo "Creating the _build and _api folders in case they do not exist"
fi


readarray -d '' pages < <(find ./_build/html/ -name '*.html' -print0)
echo "Found ${#pages[@]} HTML files."

echo "Searching links."
mapfile -t links < <(printf '%s\n' "${pages[@]}" | xargs -n 1 -P "$(nproc)" lynx -listonly -nonumbers -dump)
echo "Found ${#links[@]} links."
mapfile -t external_links < <(printf '%s\n' "${links[@]}" | grep "^https\?://" | sort | uniq)
echo "Including ${#external_links[@]} unique external links."


echo "Checking links."
invalid_links=()
i=1
for external_link in "${external_links[@]}"
do
    redraw_progress_bar 50 1 $i ${#external_links[@]}

    if ! curl --max-time 3 --silent --head "${external_link}" > /dev/null ; then
        invalid_links+=("${external_link}")
    fi
    i=$((i+1))
done
# Clear line - hide progress bar
echo -n -e "\033[2K"


if [[ ${#invalid_links[@]} -ne 0 ]]; then
    echo "Found ${#invalid_links[@]} invalid links: "
    printf '%s\n' "${invalid_links[@]}"
    exit 1
else
    echo "All links work"
fi

popd &>/dev/null || exit 1
