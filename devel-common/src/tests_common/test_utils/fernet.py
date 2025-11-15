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

import base64
import hashlib


def generate_fernet_key_string(string_key: str = "AIRFLOW_INTEGRATION_TEST") -> str:
    """Generate always the same Fernet key value as a URL-safe base64-encoded 32-byte key."""
    raw = hashlib.sha256(string_key.encode()).digest()  # 32 bytes
    return base64.urlsafe_b64encode(raw).decode()
