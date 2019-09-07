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
    echo "For futher informatiion, look at: http://lynx.browser.org/"
    exit
fi

MY_DIR="$(cd "$(dirname "$0")" && pwd)"
pushd "${MY_DIR}" &>/dev/null || exit 1

echo
echo "Working in ${MY_DIR} folder"
echo

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
