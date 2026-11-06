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
"""Importable entry point for the fork+exec end-to-end test; it runs inside the exec'd child."""

from __future__ import annotations

import sys

from airflow.sdk.execution_time.comms import CommsDecoder


def exec_probe_main() -> None:
    """Stand-in for ``_subprocess_main``: consume the startup message, then report over stdout."""
    CommsDecoder()._get_response()
    print("exec-probe-ok")
    sys.stdout.flush()
