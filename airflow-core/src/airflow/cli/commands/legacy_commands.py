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

from argparse import ArgumentError

COMMAND_MAP = {
    "dags backfill": "backfill create",
    "webserver": "api-server",
}


def check_legacy_command(action, value):
    """Check command value and raise error if value is in removed command."""
    new_command = COMMAND_MAP.get(value)
    if not hasattr(action, "_prog_prefix"):
        return
    prefix = action._prog_prefix.replace("airflow ", "")

    full_command = f"{prefix} {value}"
    if not new_command:
        new_command = COMMAND_MAP.get(full_command)
    if new_command is not None:
        msg = f"Command `{full_command}` has been removed. Please use `airflow {new_command}`"
        raise ArgumentError(action, msg)
