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

import os
import signal

from airflow.exceptions import AirflowTaskTimeout
from airflow.utils.log.logging_mixin import LoggingMixin


class timeout(LoggingMixin):  # pylint: disable=invalid-name
    """To be used in a ``with`` block and timeout its content."""

    def __init__(self, seconds=1, error_message='Timeout'):
        super().__init__()
        self.seconds = seconds
        self.error_message = error_message + ', PID: ' + str(os.getpid())

    def handle_timeout(self, signum, frame):  # pylint: disable=unused-argument
        """Logs information and raises AirflowTaskTimeout."""
        self.log.error("Process timed out, PID: %s", str(os.getpid()))
        raise AirflowTaskTimeout(self.error_message)

    def __enter__(self):
        try:
            signal.signal(signal.SIGALRM, self.handle_timeout)
            signal.setitimer(signal.ITIMER_REAL, self.seconds)
        except ValueError:
            self.log.warning("timeout can't be used in the current context", exc_info=True)

    def __exit__(self, type_, value, traceback):
        try:
            signal.setitimer(signal.ITIMER_REAL, 0)
        except ValueError:
            self.log.warning("timeout can't be used in the current context", exc_info=True)
