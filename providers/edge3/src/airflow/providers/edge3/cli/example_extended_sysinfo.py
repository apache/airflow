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
Example of an extended sysinfo function that can be used in the Edge Worker.

To enable this set the airflow config [edge] extended_system_info_function to
airflow.providers.edge3.cli.example_extended_sysinfo.get_example_extended_sysinfo
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import sys
from datetime import datetime

import psutil
from anyio import Path


async def get_example_extended_sysinfo() -> dict[str, str | int | float | datetime]:
    """Provide an example extended sysinfo function that can be used in the Edge Worker."""
    disk_usage, cpu_usage, loadavg = await asyncio.gather(
        asyncio.to_thread(shutil.disk_usage, "/"),
        asyncio.to_thread(psutil.cpu_percent, None),
        asyncio.to_thread(os.getloadavg),
    )
    disk_free_gb = round(disk_usage.free / (1024**3), 2)

    load_1 = loadavg[0]

    status = logging.INFO
    status_text: str | None = "I am good, sun is shining 🌞"
    if cpu_usage > 95 or disk_free_gb < 5:
        status = logging.ERROR
        status_text = "Critical condition!"
    elif cpu_usage > 70 or disk_free_gb < 20:
        status = logging.WARNING
        status_text = "Warning condition!"

    # For testing allowing to mock some status from file
    status_path = Path("/tmp/edge_error_status")
    if await status_path.exists():
        status = int(await status_path.read_text())
        if await Path("/tmp/edge_error_status_text").exists():
            status_text = await Path("/tmp/edge_error_status_text").read_text()
        else:
            status_text = None

    return {
        "status": status,
        **({"status_text": status_text} if status_text else {}),
        "platform": sys.platform,
        "disk_free_gb": disk_free_gb,
        "cpu_usage": cpu_usage,
        "sys_load": round(load_1, 2),
    }
