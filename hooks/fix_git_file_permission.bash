#!/bin/bash
#
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
#
MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Make sure group write/read/execute access is taken away.
# This is very important for cache as access rights are used in deciding about cache invalidation.
# Traditionlly most CI systems have umask set as 0022 and some as 0002.
# This means that when you have fresh checkout, those files have different permissions
# and they invalidate cache as calculated by docker.".
# This change removes all rights for "group" and "other" for all non .gitignored files"
# effectively acting as if umask was 0077". When consistently applied for all files in git
# this change makes cache is not accidentally invalidated across different machines.

FILE="${1}"

STAT_BIN=stat
if [[ "${OSTYPE}" == "darwin"* ]]; then
    STAT_BIN=gstat
fi

ACCESS_RIGHTS=$("${STAT_BIN}" -c "%a" "${FILE}")
if [[ "${ACCESS_RIGHTS:1:2}" != "00" ]]; then
    FILE_TYPE=$("${STAT_BIN}" -c "%F" "${FILE}")
    if [[ "${FILE_TYPE}" != "symbolic link" ]]; then
        "${STAT_BIN}" --printf "%a %A %F \t%s \t->    " "${FILE}"
        chmod og-rwx "${FILE}"
        "${STAT_BIN}" --printf "%a %A %F \t%s \t%n\n" "${FILE}"
    else
        "${STAT_BIN}" --printf "%a %A %F \t%s \t->    " "${FILE}"
        chmod og-rwx "${FILE}"
        "${STAT_BIN}" --printf "%a %A %F \t%s \t%n\n" "${FILE}"
    fi
fi
