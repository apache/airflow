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
"""Utility for setting process titles."""

from __future__ import annotations

import logging
import sys

from airflow.exceptions import AirflowException

log = logging.getLogger(__name__)


def setproctitle(title: str) -> None:
    """
    Set the process title for the current process.

    On Mac OS (darwin), this operation is skipped as the setproctitle package causes
    issues on this platform (see: https://github.com/benoitc/gunicorn/issues/3021).

    :param title: The new title for the process
    :raises AirflowException: If the setproctitle package is not installed
    """
    if sys.platform == "darwin":
        log.debug("Mac OS detected, skipping setproctitle")
        return

    try:
        import setproctitle
    except ImportError:
        raise AirflowException("The 'setproctitle' package is required to set process titles.")

    setproctitle.setproctitle(title)
