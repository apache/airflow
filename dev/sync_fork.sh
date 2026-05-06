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
#
# Sync branches on your fork with the upstream Apache Airflow repository.
#
# Force-pushes the named branches from <upstream>/<branch> directly to
# <origin>/<branch>. Does not touch your local working tree or checked-out
# branch.
#
# WARNING: this OVERWRITES the listed branches on your fork. Any commits you
# have on those branches in your fork that are not in upstream will be lost.
#
# Usage:
#   dev/sync_fork.sh [branch ...]
#
# Examples:
#   dev/sync_fork.sh                          # sync the default branches
#   dev/sync_fork.sh main                     # sync only main
#   dev/sync_fork.sh main v3-2-test v3-1-test # sync several branches
#
# The upstream and origin remote names default to "upstream" and "origin" and
# can be overridden via the UPSTREAM_REMOTE and ORIGIN_REMOTE env vars.
#
# NOTE FOR RELEASE MANAGERS: when a new vX-Y-test branch is cut, update the
# DEFAULT_BRANCHES list below to include it (and drop branches that are no
# longer maintained).
set -euo pipefail

UPSTREAM="${UPSTREAM_REMOTE:-upstream}"
ORIGIN="${ORIGIN_REMOTE:-origin}"
DEFAULT_BRANCHES=(main v3-2-test airflow-ctl/v0-1-test)

if [[ $# -gt 0 ]]; then
    BRANCHES=("$@")
else
    BRANCHES=("${DEFAULT_BRANCHES[@]}")
fi

echo "Fetching ${UPSTREAM}..."
git fetch --prune "${UPSTREAM}"

for branch in "${BRANCHES[@]}"; do
    echo "Syncing ${branch}: ${UPSTREAM}/${branch} -> ${ORIGIN}/${branch}"
    git push --force "${ORIGIN}" "refs/remotes/${UPSTREAM}/${branch}:refs/heads/${branch}"
done

echo "Done."
