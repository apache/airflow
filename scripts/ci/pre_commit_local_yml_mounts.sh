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

MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# shellcheck source=scripts/ci/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/_script_init.sh"

TMP_OUTPUT=$(mktemp)

# Remove temp file if it's hanging around
trap 'rm -rf -- "${TMP_OUTPUT}" 2>/dev/null' EXIT

LOCAL_YML_FILE="${MY_DIR}/docker-compose/local.yml"

LEAD='      # START automatically generated volumes from LOCAL_MOUNTS in _utils.sh'
TAIL='      # END automatically generated volumes from LOCAL_MOUNTS in _utils.sh'

generate_local_mounts_list "      - ../../../"

sed -ne "0,/$LEAD/ p" "${LOCAL_YML_FILE}" > "${TMP_OUTPUT}"

printf '%s\n' "${LOCAL_MOUNTS[@]}" >> "${TMP_OUTPUT}"
sed -ne "/$TAIL/,\$ p" "${LOCAL_YML_FILE}" >> "${TMP_OUTPUT}"

mv "${TMP_OUTPUT}" "${LOCAL_YML_FILE}"
