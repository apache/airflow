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

import importlib
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytest_plugins = "tests_common.pytest_plugin"


@pytest.fixture(autouse=True)
def _ensure_fake_ibmmq_if_missing():
    """Autouse fixture that injects a fake `ibmmq` module into sys.modules
    for the duration of tests in this package when the real package is not
    available. This avoids global import-time mutations while preserving test
    behavior.
    """

    if importlib.util.find_spec("ibmmq") is not None:
        # Real package available, nothing to do.
        yield
        return

    from airflow.providers.ibm.mq.hooks import mq

    # Create a minimal fake module with the attributes our tests expect.
    fake = ModuleType("ibmmq")
    # https://www.ibm.com/docs/en/ibm-mq/9.4.x?topic=20-cmqc
    fake.CMQC = SimpleNamespace(
        MQCC_OK=0,
        MQCC_WARNING=1,
        MQCC_FAILED=2,
        MQRC_NONE=0,
        MQRC_CONNECTION_BROKEN=2009,
        MQRC_Q_MGR_NOT_AVAILABLE=2059,
        MQRC_NO_MSG_AVAILABLE=2033,
        MQRC_NOT_AUTHORIZED=2035,
        MQRC_UNKNOWN_OBJECT_NAME=2085,
        MQRC_Q_MGR_NAME_ERROR=2058,
        MQRC_Q_MGR_QUIESCING=2161,
        MQRC_CONNECTION_QUIESCING=2202,
        MQRC_HOST_NOT_AVAILABLE=2538,
        MQGMO_NO=0,
        MQGMO_NO_WAIT=0,
        MQGMO_WAIT=1,
        MQGMO_NO_SYNCPOINT=2,
        MQGMO_CONVERT=4,
        MQOO_INPUT_EXCLUSIVE=4,
        MQOO_INPUT_SHARED=2,
        MQOO_FAIL_IF_QUIESCING=8192,
        MQOO_OUTPUT=0x00000010,
        MQFMT_STRING=b"MQSTR   ",
        MQENC_NATIVE=0x00000111,
    )

    class MQMIError(Exception):
        def __init__(self, comp: int | None = None, reason: int | None = None):
            self.comp = comp or fake.CMQC.MQCC_OK
            self.reason = reason or fake.CMQC.MQRC_NONE

        def __str__(self) -> str:
            return f"MQI Error. Comp {self.comp}, Reason {self.reason}: {self.error_as_string()}"

        def error_as_string(self) -> str:
            """Return the exception object MQI warning/failed reason as its mnemonic string."""
            if self.comp == fake.CMQC.MQCC_OK:
                return "OK"

            if self.comp == fake.CMQC.MQCC_WARNING:
                pfx = "WARNING"
            else:
                pfx = "FAILED"

            return f"{pfx}: Error code {self.reason} not defined"

    class PYIFError(Exception):
        def __init__(self, e=""):
            self.error = e

    fake.MQMIError = MQMIError
    fake.PYIFError = PYIFError
    fake.OD = MagicMock()
    fake.MD = MagicMock()
    fake.GMO = MagicMock()
    fake.CSP = MagicMock()
    fake.Queue = MagicMock()
    fake.connect = MagicMock()
    fake.RFH2 = MagicMock()

    sys.modules["ibmmq"] = fake
    original_mq = getattr(mq, "ibmmq", None)
    # Bind the fake into the already-imported hook module (if present) so that
    # module-level references to `ibmmq` are satisfied even if the hook was
    # imported before this fixture ran during collection.
    setattr(mq, "ibmmq", fake)

    try:
        yield
    finally:
        # Remove the injected fake to avoid leaking into other tests
        sys.modules.pop("ibmmq", None)
        setattr(mq, "ibmmq", original_mq)
