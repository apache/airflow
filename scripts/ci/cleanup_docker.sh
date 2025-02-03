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
function cleanup_docker {
    # This is faster than docker prune
    sudo systemctl stop docker
    sudo rm -rf /var/lib/docker
    # If a path is provided in ENV, bind mount it to /var/lib/docker
    if [ -n "${TARGET_DOCKER_VOLUME_LOCATION}" ]; then
        echo "Mounting ${TARGET_DOCKER_VOLUME_LOCATION} to /var/lib/docker"
        sudo mkdir -p "${TARGET_DOCKER_VOLUME_LOCATION}" /var/lib/docker
        sudo mount --bind "${TARGET_DOCKER_VOLUME_LOCATION}" /var/lib/docker
    fi
    sudo systemctl start docker
}

cleanup_docker
