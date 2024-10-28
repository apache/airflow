#!/usr/bin/env python3
#
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
from __future__ import annotations

import os
import pwd
import sys
from pathlib import Path

HOST_OS = os.environ.get("HOST_OS", "")
DOCKER_IS_ROOTLESS = os.environ.get("DOCKER_IS_ROOTLESS", "false") == "true"


def change_ownership_of_files(path: Path) -> None:
    host_user_id = os.environ.get("HOST_USER_ID", "")
    host_group_id = os.environ.get("HOST_GROUP_ID", "")
    if host_user_id == "" or host_group_id == "":
        print(
            f"ERROR: HOST_USER_ID or HOST_GROUP_ID environment variables "
            f"are not set: {host_user_id}:{host_group_id}"
        )
        sys.exit(1)
    if not host_user_id.isnumeric() or not host_group_id.isnumeric():
        print(
            f"ERROR: HOST_USER_ID or HOST_GROUP_ID environment variables "
            f"should be numeric: {host_user_id}:{host_group_id}"
        )
        sys.exit(1)
    count_files = 0
    root_uid = pwd.getpwnam("root").pw_uid
    for file in path.rglob("*"):
        try:
            if file.is_symlink() and file.lstat().st_uid == root_uid:
                # Change ownership of symlink itself (by default stat/chown follow the symlinks)
                os.chown(
                    file, int(host_user_id), int(host_group_id), follow_symlinks=False
                )
                count_files += 1
                if os.environ.get("VERBOSE_COMMANDS", "false") == "true":
                    print(f"Changed ownership of symlink {file}")
            if file.stat().st_uid == root_uid:
                # And here change ownership of the file (or if it is a symlink - the file it points to)
                os.chown(file, int(host_user_id), int(host_group_id))
                count_files += 1
                if os.environ.get("VERBOSE_COMMANDS", "false") == "true":
                    print(f"Changed ownership of {file.resolve()}")
        except FileNotFoundError:
            # This is OK - file might have been deleted in the meantime or linked in Host from
            # another place
            if os.environ.get("VERBOSE_COMMANDS", "false") == "true":
                print(f"Could not change ownership of {file}")
    if count_files:
        print(
            f"Changed ownership of {count_files} files back to {host_user_id}:{host_group_id}."
        )


if __name__ == "__main__":
    if HOST_OS == "":
        print("ERROR: HOST_OS environment variable is not set")
        sys.exit(1)
    if HOST_OS != "linux":
        print("Since host OS is not Linux, we don't need to fix ownership.")
        sys.exit(0)
    if DOCKER_IS_ROOTLESS:
        print(
            "Since Docker is in rootless mode , we don't need to fix ownership even on Linux."
        )
        sys.exit(0)

    change_ownership_of_files(Path("/opt/airflow"))
