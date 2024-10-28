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

import platform
import sys
from pathlib import Path


def get_real_platform(single_platform: str) -> str:
    """
    Replace different platform variants of the platform provided platforms with the two canonical ones we
    are using: amd64 and arm64.
    """
    return (
        single_platform.replace("x86_64", "amd64")
        .replace("aarch64", "arm64")
        .replace("/", "-")
    )


def _exists_no_permission_error(p: str) -> bool:
    try:
        return Path(p).exists()
    except PermissionError:
        return False


def message_on_wsl1_detected(
    release_name: str | None, kernel_version: tuple[int, ...] | None
):
    from airflow_breeze.utils.console import get_console

    get_console().print("[error]You are running WSL1 - Breeze requires WSL2! Quitting.\n")
    get_console().print(
        "[warning]It can also be that our detection mechanism is wrong:[/]\n\n"
    )
    if release_name:
        get_console().print(
            f"[info]We based our WSL1 detection on the release name: `{release_name}`\n"
        )
    elif kernel_version:
        get_console().print(
            f"[info]We based our WSL1 detection on the kernel version: `{kernel_version}`\n"
        )
    get_console().print(
        "[info]If you are running WSL2, please report this issue to the maintainers\n"
        "of Airflow, so we can improve the detection mechanism.\n"
        "You can also try to run the command with `--uv-http-timeout 900` or "
        "`--no-use-uv` flag to skip the WSL1 check.\n"
    )


def is_wsl2() -> bool:
    """
    Check if the current platform is WSL2. This method will exit with error printing appropriate
    message if WSL1 is detected as WSL1 is not supported.

    :return: True if the current platform is WSL2, False otherwise (unless it's WSL1 then it exits).
    """
    if not sys.platform.startswith("linux"):
        return False
    release_name = platform.uname().release
    has_wsl_interop = _exists_no_permission_error("/proc/sys/fs/binfmt_misc/WSLInterop")
    microsoft_in_release = "microsoft" in release_name.lower()
    wsl_conf = _exists_no_permission_error("/etc/wsl.conf")
    if not has_wsl_interop and not microsoft_in_release and not wsl_conf:
        return False
    if microsoft_in_release:
        # Release name WSL1 detection
        if "Microsoft" in release_name:
            message_on_wsl1_detected(release_name=release_name, kernel_version=None)
            sys.exit(1)
        return True

    # Kernel WSL1 detection
    kernel_version: tuple[int, ...] = (0, 0)
    if len(parts := release_name.split(".", 2)[:2]) == 2:
        try:
            kernel_version = tuple(map(int, parts))
        except (TypeError, ValueError):
            pass
    if kernel_version < (4, 19):
        message_on_wsl1_detected(release_name=None, kernel_version=kernel_version)
        sys.exit(1)
    return True
