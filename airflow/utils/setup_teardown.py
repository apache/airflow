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

import contextlib
import enum

from airflow.exceptions import AirflowException


class SetupTeardown(enum.Enum):
    """Flag to indicate a setup/teardown context."""

    setup = "setup"
    teardown = "teardown"


class SetupTeardownContext:
    """Track whether the next added task is a setup or teardown task."""

    setup_teardown: SetupTeardown | None = None

    @classmethod
    @contextlib.contextmanager
    def setup(cls):
        if cls.setup_teardown is not None:
            raise AirflowException(
                "A setup task or task group cannot be nested inside another setup/teardown task or taskgroup"
            )

        cls.setup_teardown = SetupTeardown.setup
        try:
            yield
        finally:
            cls.setup_teardown = None

    @classmethod
    @contextlib.contextmanager
    def teardown(cls):
        if cls.setup_teardown is not None:
            raise AirflowException(
                "A teardown task or task group cannot be nested inside another"
                " setup/teardown task or taskgroup"
            )

        cls.setup_teardown = SetupTeardown.teardown
        try:
            yield
        finally:
            cls.setup_teardown = None
