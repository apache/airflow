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
import signal
import sys

from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.definitions.dag import dag


class SignalForwardOperator(BaseOperator):
    """Operator that sends SIGTERM to its parent (the supervisor) to test signal forwarding."""

    def execute(self, context):
        # Print sentinel so the test knows execute() is running
        print("EXECUTE_STARTED", flush=True)
        # Send SIGTERM to the supervisor (parent process)
        os.kill(os.getppid(), signal.SIGTERM)
        # Keep running long enough for the signal to be forwarded back and on_kill() to fire
        import time

        time.sleep(30)

    def on_kill(self) -> None:
        print("ON_KILL_CALLED_VIA_SIGNAL_FORWARDING", flush=True)
        sys.exit(0)


@dag()
def signal_forward_test():
    SignalForwardOperator(task_id="signal_task")


signal_forward_test()
