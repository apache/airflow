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

function usage() {
    CMDNAME="$(basename -- "$0")"

      echo """
Usage: ${CMDNAME} <username> <password>

Creates an account for the administrator.
"""
}

if [[ ! "$#" -eq 2 ]]; then
    echo "You must provide exactly two arguments."
    usage
    exit 1
fi

USERNAME=$1
PASSWORD=$2

REALM_NAME=EXAMPLE.COM

cat << EOF | kadmin.local &>/dev/null
add_principal -pw $PASSWORD "${USERNAME}/admmin@${REALM_NAME}"
listprincs
quit
EOF

echo "Created admin: ${USERNAME}"
