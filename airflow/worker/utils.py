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
This module contains helpers for airflow workers.
"""

import signal
import uuid
from typing import Callable

WORKER_PID_FILE_LOCATION = "/tmp/airflow_worker.pid"


def unique_suffix(name: str, suffix_len: int = 7) -> str:
    """
    Adds unique suffix based on uuid.
    """
    return f"{name}-{str(uuid.uuid4())[:suffix_len]}"


def add_signal_handler(func: Callable) -> None:
    """
    Register a SIGINT and SIGTERM handler.
    """
    signal.signal(signal.SIGINT, func)
    signal.signal(signal.SIGTERM, func)
