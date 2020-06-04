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

SCRIPTS_CI_DIR=$(dirname "${BASH_SOURCE[0]}")

# must be first to initialize arrays TODO: For sure?
# shellcheck source=scripts/ci/libraries/_initialization.sh
. "${SCRIPTS_CI_DIR}"/libraries/_initialization.sh


# shellcheck source=scripts/ci/libraries/_sanity_checks.sh
. "${SCRIPTS_CI_DIR}"/libraries/_sanity_checks.sh
# shellcheck source=scripts/ci/libraries/_build_images.sh
. "${SCRIPTS_CI_DIR}"/libraries/_build_images.sh
# shellcheck source=scripts/ci/libraries/_kind.sh
. "${SCRIPTS_CI_DIR}"/libraries/_kind.sh
# shellcheck source=scripts/ci/libraries/_local_mounts.sh
. "${SCRIPTS_CI_DIR}"/libraries/_local_mounts.sh
# shellcheck source=scripts/ci/libraries/_md5sum.sh
. "${SCRIPTS_CI_DIR}"/libraries/_md5sum.sh
# shellcheck source=scripts/ci/libraries/_parameters.sh
. "${SCRIPTS_CI_DIR}"/libraries/_parameters.sh
# shellcheck source=scripts/ci/libraries/_permissions.sh
. "${SCRIPTS_CI_DIR}"/libraries/_permissions.sh
# shellcheck source=scripts/ci/libraries/_push_pull_remove_images.sh
. "${SCRIPTS_CI_DIR}"/libraries/_push_pull_remove_images.sh
# shellcheck source=scripts/ci/libraries/_pylint.sh
. "${SCRIPTS_CI_DIR}"/libraries/_pylint.sh
# shellcheck source=scripts/ci/libraries/_runs.sh
. "${SCRIPTS_CI_DIR}"/libraries/_runs.sh
# shellcheck source=scripts/ci/libraries/_spinner.sh
. "${SCRIPTS_CI_DIR}"/libraries/_spinner.sh
# shellcheck source=scripts/ci/libraries/_start_end.sh
. "${SCRIPTS_CI_DIR}"/libraries/_start_end.sh
# shellcheck source=scripts/ci/libraries/_verbosity.sh
. "${SCRIPTS_CI_DIR}"/libraries/_verbosity.sh
