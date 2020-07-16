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

get_environment_for_builds_on_ci

git remote add target "https://github.com/${CI_TARGET_REPO}"

git fetch target "${CI_TARGET_BRANCH}:${CI_TARGET_BRANCH}" --depth=1

CHANGED_FILES=$(git diff-tree --no-commit-id --name-only -r "${1}" "${CI_TARGET_BRANCH}" || true)

echo
echo "Changed files:"
echo
echo "${CHANGED_FILES}"
echo

echo
echo "Changed files matching the ${2} pattern"
echo
echo "${CHANGED_FILES}" | grep -E "${2}" || true
echo

echo
echo "Count changed files matching the ${2} pattern"
echo
COUNT_CHANGED_FILES=$(echo "${CHANGED_FILES}" | grep -c -E "${2}" || true)
echo "${COUNT_CHANGED_FILES}"
echo

exit "${COUNT_CHANGED_FILES}"
