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
function cleanup_runner {
    set -x
    local target_docker_volume_location="/mnt/var-lib-docker"
    echo "Checking free space!"
    df -H
    # This is faster than docker prune
    echo "Stopping docker"
    sudo systemctl stop docker
    echo "Checking free space!"
    df -H
    echo "Cleaning docker"
    sudo rm -rf /var/lib/docker
    echo "Checking free space!"
    df -H
    echo "Mounting ${target_docker_volume_location} to /var/lib/docker"
    sudo mkdir -p "${target_docker_volume_location}" /var/lib/docker
    sudo mount --bind "${target_docker_volume_location}" /var/lib/docker
    sudo chown -R 0:0 "${target_docker_volume_location}"
    sudo systemctl start docker
    echo "Checking free space!"
    df -H
}

cleanup_runner
