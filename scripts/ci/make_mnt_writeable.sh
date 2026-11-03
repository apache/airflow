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
function make_mnt_writeable {
    set -x
    echo "Investigating node disks"
    lsblk
    sudo blkid
    echo "Checking free space!"
    df -H
    echo "Cleaning /mnt just in case it is not empty and making sure that /mnt is writeable"
    sudo mkdir -p /mnt
    if [[ "${RUNNER_ENVIRONMENT:-}" == "self-hosted" ]]; then
        # RunsOn mounts an instance's local NVMe disks under /mnt and keeps docker, /home/runner and
        # /tmp on them, so emptying or re-owning such a mount would break the running job.
        local entry
        for entry in /mnt/*; do
            if mountpoint -q "${entry}"; then
                echo "Keeping mount point ${entry}"
            else
                sudo rm -rf --one-file-system "${entry}"
            fi
        done
        sudo chown "${USER}" /mnt
    else
        sudo rm -rf /mnt/*
        sudo chown -R "${USER}" /mnt
    fi
    echo "Checking free space!"
    df -H
}

make_mnt_writeable
