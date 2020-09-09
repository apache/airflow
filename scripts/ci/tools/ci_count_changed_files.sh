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

# Returns number of files matching the pattern changed in revision specified
# Versus the tip of the target branch
# Parameters
#  $1: Revision to compare
#  $2: Pattern to match
# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

if [[ ${CI_EVENT_TYPE} == "push" ]]; then
    echo
    echo "Always run all tests on push"
    echo
    exit 1
fi

git remote add target "https://github.com/${CI_TARGET_REPO}"

git fetch target "${CI_TARGET_BRANCH}:${CI_TARGET_BRANCH}" --depth=1

echo
echo "Retrieve changed files from ${COMMIT_SHA} comparing to ${CI_TARGET_BRANCH}"
echo

changed_files=$(git diff-tree --no-commit-id --name-only -r "${COMMIT_SHA}" "${CI_TARGET_BRANCH}" || true)

echo
echo "Changed files:"
echo
echo "${changed_files}"
echo

echo
echo "Changed files matching the ${1} pattern"
echo
echo "${changed_files}" | grep -E "${1}" || true
echo

echo
echo "Count changed files matching the ${1} pattern"
echo
count_changed_files=$(echo "${changed_files}" | grep -c -E "${1}" || true)
echo "${count_changed_files}"
echo

exit "${count_changed_files}"
