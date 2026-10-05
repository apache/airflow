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
from __future__ import annotations

import subprocess
import sys
import textwrap


def test_shared_cache_runs_without_core_or_sdk():
    code = textwrap.dedent(
        """
        import sys

        class NoAirflowImports:
            def find_spec(self, fullname, path=None, target=None):
                if fullname == "airflow" or fullname.startswith("airflow."):
                    raise ModuleNotFoundError(fullname, name=fullname)

        sys.meta_path.insert(0, NoAirflowImports())

        from airflow_shared.secrets_backend.cache import SecretCache

        SecretCache._initialize(ttl_seconds=30)
        SecretCache.save_variable("key", "value", team_name="team")
        assert SecretCache.get_variable("key", team_name="team") == "value"
        SecretCache.save_connection_uri("connection", "http://example.com")
        assert SecretCache.get_connection_uri("connection") == "http://example.com"
        """
    )
    subprocess.run([sys.executable, "-c", code], check=True, timeout=30)
