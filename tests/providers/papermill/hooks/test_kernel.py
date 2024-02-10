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

from unittest.mock import patch

from airflow.models import Connection
from airflow.providers.papermill.hooks.kernel import KernelHook


class TestKernelHook:
    """
    Tests for Kernel connection
    """

    def test_kernel_connection(self):
        """
        Test that fetches kernelConnection with configured host and ports
        """
        conn = Connection(
            conn_type="jupyter_kernel", host="test_host", extra='{"shell_port": 60000, "session_key": "key"}'
        )
        with patch.object(KernelHook, "get_connection", return_value=conn):
            hook = KernelHook()
        assert hook.get_conn().ip == "test_host"
        assert hook.get_conn().shell_port == 60000
        assert hook.get_conn().session_key == "key"
