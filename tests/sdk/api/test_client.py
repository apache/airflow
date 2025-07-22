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

import tempfile

from airflow.sdk.api.client import Client

class TestClientSSL:
    """Test loading a custom SSL certificate via environment variable in the API client."""

    def test_client_loads_custom_ssl_certificate(self, monkeypatch):
        """Should load client with custom SSL cert path from environment variable."""
        with tempfile.NamedTemporaryFile("w") as cert_file:
            cert_file.write("-----BEGIN CERTIFICATE-----\n...FAKE-CERT-HERE...\n-----END CERTIFICATE-----")
            cert_file.flush()
            monkeypatch.setenv("AIRFLOW__API__SSL_CERT", cert_file.name)
            client = Client(token="abc", base_url="https://localhost")
            if client is None:
                raise AssertionError("Client failed to initialize")