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

# In case "VERBOSE" is set to "true" (--verbose flag in Breeze) all docker commands run will be
# printed before execution
function verbose_docker {
    if [[ ${VERBOSE:="false"} == "true" && \
        # do not print echo if VERBOSE_COMMAND is set (set -x does it already)
        ${VERBOSE_COMMANDS:=} != "true" && \
        # And when generally printing info is disabled
        ${PRINT_INFO_FROM_SCRIPTS} == "true" ]]; then
        >&2 echo "docker" "${@}"
    fi
    if [[ ${PRINT_INFO_FROM_SCRIPTS} == "false" ]]; then
        docker "${@}" >>"${OUTPUT_LOG}" 2>&1
    else
        docker "${@}" 2>&1 | tee -a "${OUTPUT_LOG}"
    fi
    EXIT_CODE="$?"
    if [[ ${EXIT_CODE} == "0" ]]; then
        # No matter if "set -e" is used the log will be removed on success.
        # This way in the output log we only see the most recent failed command and what was echoed before
        rm -f "${OUTPUT_LOG}"
    fi
    return "${EXIT_CODE}"
}

# In case "VERBOSE" is set to "true" (--verbose flag in Breeze) all helm commands run will be
# printed before execution
function verbose_helm {
    if [[ ${VERBOSE:="false"} == "true" && ${VERBOSE_COMMANDS:=} != "true" ]]; then
       # do not print echo if VERBOSE_COMMAND is set (set -x does it already)
        >&2 echo "helm" "${@}"
    fi
    helm "${@}" | tee -a "${OUTPUT_LOG}"
    if [[ ${EXIT_CODE} == "0" ]]; then
        # No matter if "set -e" is used the log will be removed on success.
        rm -f "${OUTPUT_LOG}"
    fi
}

# In case "VERBOSE" is set to "true" (--verbose flag in Breeze) all kubectl commands run will be
# printed before execution
function verbose_kubectl {
    if [[ ${VERBOSE:="false"} == "true" && ${VERBOSE_COMMANDS:=} != "true" ]]; then
       # do not print echo if VERBOSE_COMMAND is set (set -x does it already)
        >&2 echo "kubectl" "${@}"
    fi
    kubectl "${@}" | tee -a "${OUTPUT_LOG}"
    if [[ ${EXIT_CODE} == "0" ]]; then
        # No matter if "set -e" is used the log will be removed on success.
        rm -f "${OUTPUT_LOG}"
    fi
}

# In case "VERBOSE" is set to "true" (--verbose flag in Breeze) all kind commands run will be
# printed before execution
function verbose_kind {
    if [[ ${VERBOSE:="false"} == "true" && ${VERBOSE_COMMANDS:=} != "true" ]]; then
       # do not print echo if VERBOSE_COMMAND is set (set -x does it already)
        >&2 echo "kind" "${@}"
    fi
    # kind outputs nice output on terminal.
    kind "${@}"
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

alias docker=verbose_docker
alias kubectl=verbose_kubectl
alias helm=verbose_helm
alias kind=verbose_kind
