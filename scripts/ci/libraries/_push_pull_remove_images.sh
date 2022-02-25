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


# Tries to push the image several times in case we receive an intermittent error on push
# $1 - tag to push
function push_pull_remove_images::push_image_with_retries() {
    for try_num in 1 2 3 4
    do
        set +e
        echo
        echo "Trying to push the image ${1}. Number of try: ${try_num}"
        docker_v push "${1}"
        local res=$?
        set -e
        if [[ ${res} != "0" ]]; then
            echo
            echo  "${COLOR_YELLOW}WARNING: Error ${res} when pushing image on ${try_num} try  ${COLOR_RESET}"
            echo
            continue
        else
            return 0
        fi
    done
    echo
    echo  "${COLOR_RED}ERROR: Error ${res} when pushing image on ${try_num} try. Giving up!  ${COLOR_RESET}"
    echo
    return 1
}


# Pulls image in case it is missing
# Should be run with set +e
# Parameters:
#   $1 -> image to pull
function push_pull_remove_images::pull_image_if_missing() {
    local image_to_pull="${1}"
    local image_hash
    image_hash=$(docker images -q "${image_to_pull}" 2> /dev/null || true)
    if [[ -z "${image_hash=}" ]]; then
        echo
        echo "Pulling the image ${image_to_pull}"
        echo
        docker pull "${image_to_pull}"
    fi
}


# waits for an image to be available in the GitHub registry
function push_pull_remove_images::wait_for_image() {
    # Maximum number of tries 100 = we try for max. 100 minutes.
    local MAX_TRIES=100
    set +e
    echo " Waiting for github registry image: $1"
    local count=0
    while true
    do
        if push_pull_remove_images::pull_image_if_missing "$1"; then
            break
        fi
        if [[ ${count} == "${MAX_TRIES}" ]]; then
            echo "${COLOR_RED}Giving up after ${MAX_TRIES}!${COLOR_RESET}"
            echo "If there were delays with building the image, maintainers could potentially restart the build when the images are ready!"
            echo "Or you can run 'git commit --amend' and then push the PR again with 'git push --force-with-lease' to re-trigger the build."
            return 1
        fi
        echo "${COLOR_YELLOW}Failed to pull the image for ${count} time. Sleeping!${COLOR_RESET}"
        sleep 60
        count=$((count + 1))
    done
    set -e
}
