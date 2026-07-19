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


def parse_and_validate_port(port: int | str | None) -> int | None:
    """Parse and validate connection port range."""
    if port is None:
        return None
    if isinstance(port, str) and not port.strip():
        return None
    try:
        port_val = int(port)
    except (ValueError, TypeError):
        raise ValueError(f"Expected integer value for `port`, but got {port!r} instead.")
    if not (1 <= port_val <= 65535):
        raise ValueError(
            f"The `port` field must be a value between 1 and 65535, but got {port_val!r} instead."
        )
    return port_val
