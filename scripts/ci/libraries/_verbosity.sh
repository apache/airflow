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
# In case "VERBOSE_COMMANDS" is set to "true" set -x is used to enable debugging
function check_verbose_setup {
    if [[ ${VERBOSE_COMMANDS:="false"} == "true" ]]; then
        set -x
    else
        set +x
    fi
}

DOCKER_BINARY="${DOCKER_BINARY:=$(command -v docker || echo "/bin/docker")}"
export DOCKER_BINARY

# In case "VERBOSE" is set to "true" (--verbose flag in Breeze) all docker commands run will be
# printed before execution
function docker {
    if [[ ${VERBOSE:="false"} == "true" && \
        # do not print echo if VERBOSE_COMMAND is set (set -x does it already)
        ${VERBOSE_COMMANDS:=} != "true" && \
        # And when generally printing info is disabled
        ${PRINT_INFO_FROM_SCRIPTS} == "true" ]]; then
        >&2 echo "docker" "${@}"
    fi
    if [[ ${PRINT_INFO_FROM_SCRIPTS} == "false" ]]; then
        ${DOCKER_BINARY} "${@}" >>"${OUTPUT_LOG}" 2>&1
    else
        ${DOCKER_BINARY} "${@}" 2>&1 | tee -a "${OUTPUT_LOG}"
    fi
    EXIT_CODE="$?"
    if [[ ${EXIT_CODE} == "0" ]]; then
        # No matter if "set -e" is used the log will be removed on success.
        # This way in the output log we only see the most recent failed command and what was echoed before
        rm -f "${OUTPUT_LOG}"
    fi
    return "${EXIT_CODE}"
}

HELM_BINARY="${HELM_BINARY:=$(command -v helm || echo "/bin/helm")}"
export HELM_BINARY

# In case "VERBOSE" is set to "true" (--verbose flag in Breeze) all helm commands run will be
# printed before execution
function helm {
    if [[ ${VERBOSE:="false"} == "true" && ${VERBOSE_COMMANDS:=} != "true" ]]; then
       # do not print echo if VERBOSE_COMMAND is set (set -x does it already)
        >&2 echo "helm" "${@}"
    fi
    ${HELM_BINARY} "${@}" | tee -a "${OUTPUT_LOG}"
    if [[ ${EXIT_CODE} == "0" ]]; then
        # No matter if "set -e" is used the log will be removed on success.
        rm -f "${OUTPUT_LOG}"
    fi
}

KUBECTL_BINARY=${KUBECTL_BINARY:=$(command -v kubectl || echo "/bin/kubectl")}
export KUBECTL_BINARY

# In case "VERBOSE" is set to "true" (--verbose flag in Breeze) all kubectl commands run will be
# printed before execution
function kubectl {
    if [[ ${VERBOSE:="false"} == "true" && ${VERBOSE_COMMANDS:=} != "true" ]]; then
       # do not print echo if VERBOSE_COMMAND is set (set -x does it already)
        >&2 echo "kubectl" "${@}"
    fi
    ${KUBECTL_BINARY} "${@}" | tee -a "${OUTPUT_LOG}"
    if [[ ${EXIT_CODE} == "0" ]]; then
        # No matter if "set -e" is used the log will be removed on success.
        rm -f "${OUTPUT_LOG}"
    fi
}

KIND_BINARY="${KIND_BINARY:=$(command -v kind || echo "/bin/kind")}"
export KIND_BINARY

# In case "VERBOSE" is set to "true" (--verbose flag in Breeze) all kind commands run will be
# printed before execution
function kind {
    if [[ ${VERBOSE:="false"} == "true" && ${VERBOSE_COMMANDS:=} != "true" ]]; then
       # do not print echo if VERBOSE_COMMAND is set (set -x does it already)
        >&2 echo "kind" "${@}"
    fi
    # kind outputs nice output on terminal.
    ${KIND_BINARY} "${@}"
}

# Prints verbose information in case VERBOSE variable is set
function print_info() {
    if [[ ${VERBOSE:="false"} == "true" && ${PRINT_INFO_FROM_SCRIPTS} == "true" ]]; then
        echo "$@"
    fi
}

function set_verbosity() {
    # whether verbose output should be produced
    export VERBOSE=${VERBOSE:="false"}

    # whether every bash statement should be printed as they are executed
    export VERBOSE_COMMANDS=${VERBOSE_COMMANDS:="false"}

    # whether the output from script should be printed at all
    export PRINT_INFO_FROM_SCRIPTS=${PRINT_INFO_FROM_SCRIPTS:="true"}
}

set_verbosity
