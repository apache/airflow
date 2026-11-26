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
"""Connection settings of the Polaris integration server (Breeze integration ``polaris``)."""

from __future__ import annotations

import os

# Breeze serves the integration as http://polaris:8181; outside Breeze, point this at the server.
URL = os.environ.get("POLARIS_INTEGRATION_URL", "http://polaris:8181")
# The server's bootstrap credentials (POLARIS_BOOTSTRAP_CREDENTIALS in integration-polaris.yml).
CLIENT_ID = "root"
CLIENT_SECRET = "s3cr3t"
REALM = "POLARIS"
