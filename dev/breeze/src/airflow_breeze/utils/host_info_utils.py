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
"""
Returns information about Host that should be passed to the docker-compose.
"""

from __future__ import annotations

import platform
from enum import Enum


class Architecture(Enum):
    X86_64 = "x86_64"
    X86 = "x86"
    PPC = "ppc"
    ARM = "arm"


def get_host_user_id() -> str:
    from airflow_breeze.utils.run_utils import run_command

    host_user_id = ""
    os = get_host_os()
    if os == "linux" or os == "darwin":
        host_user_id = run_command(
            cmd=["id", "-ur"], capture_output=True, text=True
        ).stdout.strip()
    return host_user_id


def get_host_group_id() -> str:
    from airflow_breeze.utils.run_utils import run_command

    host_group_id = ""
    os = get_host_os()
    if os == "linux" or os == "darwin":
        host_group_id = run_command(
            cmd=["id", "-gr"], capture_output=True, text=True
        ).stdout.strip()
    return host_group_id


def get_host_os() -> str:
    return platform.system().lower()


_MACHINE_TO_ARCHITECTURE: dict[str, Architecture] = {
    "amd64": Architecture.X86_64,
    "x86_64": Architecture.X86_64,
    "i686-64": Architecture.X86_64,
    "i386": Architecture.X86,
    "i686": Architecture.X86,
    "x86": Architecture.X86,
    "ia64": Architecture.X86,  # Itanium is different x64 arch, treat it as the common x86.
    "powerpc": Architecture.PPC,
    "power macintosh": Architecture.PPC,
    "ppc64": Architecture.PPC,
    "armv6": Architecture.ARM,
    "armv6l": Architecture.ARM,
    "arm64": Architecture.ARM,
    "armv7": Architecture.ARM,
    "armv7l": Architecture.ARM,
    "aarch64": Architecture.ARM,
}


def get_host_architecture() -> tuple[Architecture | None, str]:
    """Get architecture in the form of Tuple: standardized architecture, original platform"""
    machine = platform.machine()
    return _MACHINE_TO_ARCHITECTURE.get(machine.lower()), machine
