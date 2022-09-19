#!/usr/bin/env python

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

from dataclasses import dataclass

import psutil
from rich.console import Console


@dataclass
class Resource:
    current: int
    minimumAllowed: int


console = Console(force_terminal=True, color_system="standard", width=180)


def get_size(bytes):
    """
    Convert Bytes into Gigabytes
    1 Gigabytes = 1024*1024*1024 = 1073741824 bytes
    """
    factor = 1024**3
    value_gb = bytes / factor
    return value_gb


def resoure_check():
    """
    Use gsutil to get resources in bytes for memory and disk
    """
    MINIMUM_ALLOWED_MEMORY = 4
    MINIMUM_ALLOWED_CPUS = 2
    MINIMUM_ALLOWED_DISK = 20
    print("\nChecking resources.\n")

    # Memory current available
    svmem = psutil.virtual_memory()
    mem_available = round(get_size(svmem.available))

    # Cpus current available
    cpus_available = psutil.cpu_count(logical=True)

    # Disk current available
    partition_usage = psutil.disk_usage('/')
    disk_available = round(get_size(partition_usage.free))

    resources: dict[str, Resource] = {
        'Memory': Resource(current=mem_available, minimumAllowed=MINIMUM_ALLOWED_MEMORY),
        'Cpus': Resource(current=cpus_available, minimumAllowed=MINIMUM_ALLOWED_CPUS),
        'Disk': Resource(current=disk_available, minimumAllowed=MINIMUM_ALLOWED_DISK),
    }
    return resources


def resoure_validate():

    resources = resoure_check()
    warning_resources = False
    check = "OK"

    for resource, capacity in resources.items():

        check = '' if resource == "Cpus" else 'GB'

        if capacity.current < capacity.minimumAllowed:
            console.print(f"[yellow]WARNING!!!: Not enough {resource} available for Docker.")
            print(
                f"At least {capacity.minimumAllowed}{check} of {resource} required. "
                f" You have {capacity.current}{check}\n"
            )
            warning_resources = True
        else:
            console.print(f" * {resource} available {capacity.current}{check}. [green]OK.")

    if warning_resources:
        console.print("[yellow]WARNING!!!: You have not enough resources to run Airflow (see above)!")
        print("Please follow the instructions to increase amount of resources available:")
        console.print(
            " Please check https://github.com/apache/airflow/blob/main/BREEZE.rst#resources-required"
            " for details"
        )
    else:
        console.print("\n[green]Resource check successful.\n")


if __name__ == "__main__":
    resoure_validate()
