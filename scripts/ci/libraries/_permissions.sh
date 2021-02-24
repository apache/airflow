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


# Fixing permissions for all important files that are going to be added to Docker context
# This is necessary, because there are different default umask settings on different *NIX
# In case of some systems (especially in the CI environments) there is default +w group permission
# set automatically via UMASK when git checkout is performed.
#    https://unix.stackexchange.com/questions/315121/why-is-the-default-umask-002-or-022-in-many-unix-systems-seems-insecure-by-defa
# Unfortunately default setting in git is to use UMASK by default:
#    https://git-scm.com/docs/git-config/1.6.3.1#git-config-coresharedRepository
# This messes around with Docker context invalidation because the same files have different permissions
# and effectively different hash used for context validation calculation.
#
# We fix it by removing write permissions for other/group for all files that are in the Docker context.
#
# Since we can't (easily) tell what dockerignore would restrict, we'll just to
# it to "all" files in the git repo, making sure to exclude the www/static/docs
# symlink which is broken until the docs are built.
function permissions::filterout_deleted_files {
  # Take NUL-separated stdin, return only files that exist on stdout NUL-separated
  # This is to cope with users deleting files or folders locally but not doing `git rm`
  xargs -0 "$STAT_BIN" --printf '%n\0' 2>/dev/null || true;
}

# Fixes permissions for groups for all the files that are quickly filtered using the permissions::filterout_deleted_files
function permissions::fix_group_permissions() {
    if [[ ${PERMISSIONS_FIXED:=} == "true" ]]; then
        return
    fi
    pushd "${AIRFLOW_SOURCES}" >/dev/null 2>&1 || exit 1
    # This deals with files
    git ls-files -z -- ./ | permissions::filterout_deleted_files | xargs -0 chmod og-w
    # and this deals with directories
    git ls-tree -z -r -d --name-only HEAD | permissions::filterout_deleted_files | xargs -0 chmod og-w,og+x
    popd >/dev/null 2>&1 || exit 1
    export PERMISSIONS_FIXED="true"
}
