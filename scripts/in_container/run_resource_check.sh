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
# shellcheck source=scripts/in_container/_in_container_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/_in_container_script_init.sh"

function resource_check() {
    local one_meg=1048576
    local mem_available
    local cpus_available
    local disk_available
    local warning_resources="false"
    echo
    echo "Checking resources."
    echo
    mem_available=$(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE) / one_meg))
    cpus_available=$(grep -cE 'cpu[0-9]+' /proc/stat)
    disk_available=$(df / | tail -1 | awk '{print $4}')
    human_readable_memory=$(numfmt --to iec $((mem_available * one_meg)))
    human_readable_disk=$(numfmt --to iec $((disk_available * 1024 )))
    if (( mem_available < 4000 )) ; then
        echo "${COLOR_YELLOW}WARNING!!!: Not enough memory available for Docker.${COLOR_RESET}"
        echo "At least 4GB of memory required. You have ${human_readable_memory}"
        warning_resources="true"
    else
        echo "* Memory available ${human_readable_memory}. ${COLOR_GREEN}OK.${COLOR_RESET}"
    fi
    if (( cpus_available < 2 )); then
        echo "${COLOR_YELLOW}WARNING!!!: Not enough CPUS available for Docker.${COLOR_RESET}"
        echo "At least 2 CPUs recommended. You have ${cpus_available}"
        warning_resources="true"
    else
        echo "* CPUs available ${cpus_available}. ${COLOR_GREEN}OK.${COLOR_RESET}"
    fi
    if (( disk_available < one_meg*40 )); then
        echo "${COLOR_YELLOW}WARNING!!!: Not enough Disk space available for Docker.${COLOR_RESET}"
        echo "At least 40 GBs recommended. You have ${human_readable_disk}"
        warning_resources="true"
    else
        echo "* Disk available ${human_readable_disk}. ${COLOR_GREEN}OK.${COLOR_RESET}"
    fi
    if [[ ${warning_resources} == "true" ]]; then
        echo
        echo "${COLOR_YELLOW}WARNING!!!: You have not enough resources to run Airflow (see above)!${COLOR_RESET}"
        echo "Please follow the instructions to increase amount of resources available:"
        echo "   Please check https://github.com/apache/airflow/blob/main/BREEZE.rst#resources-required for details"
        echo
    else
        echo
        echo "${COLOR_GREEN}Resource check successful.${COLOR_RESET}"
        echo
    fi
}

resource_check
