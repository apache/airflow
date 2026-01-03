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
    echo "Check that we have expected /mnt to be a separate mount"
    if ! lsblk | grep -q /mnt; then
        echo "!!!! /mnt is missing as a separate mount, runner misconfigured!"
        echo "Creating /mnt drive hoping that it will be enough space to use in /"
        sudo mkdir -p /mnt/
    fi
    echo "Checking free space!"
    df -H
    echo "Cleaning /mnt just in case it is not empty"

    # Display TXT file
    cat /mnt/DATALOSS_WARNING_README.txt
    exit 42


    sudo rm -rf /mnt/*
    echo "Checking free space!"
    df -H
    echo "Making sure that /mnt is writeable"
    sudo chown -R "${USER}" /mnt
}

make_mnt_writeable
