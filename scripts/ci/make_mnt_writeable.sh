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
    set -euo pipefail
    set -x
    echo "Investigating node disks"
    # Probes are diagnostic; an unformatted optional disk must not abort setup.
    lsblk || true
    sudo blkid || true
    echo "Checking free space!"
    df -H
    echo "Cleaning /mnt just in case it is not empty"
    sudo mkdir -p /mnt
    # Keep the existing cleanup semantics: the shell glob removes the normal entries. In the
    # common case /mnt is then empty and only the mountpoint needs chowning. If hidden entries
    # remain, preserve the old recursive ownership behavior for those entries.
    sudo rm -rf /mnt/*
    echo "Making sure that /mnt is writeable"
    local remaining
    # Assign outside the conditional so a failed scan cannot be mistaken for an empty directory.
    remaining="$(sudo find /mnt -mindepth 1 -maxdepth 1 -print -quit)"
    if [[ -n "${remaining}" ]]; then
        sudo chown -R "${USER}" /mnt
    else
        sudo chown "${USER}" /mnt
    fi
    echo "Checking free space!"
    df -H
}

make_mnt_writeable
