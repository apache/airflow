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
"""HBase connection configuration."""

from __future__ import annotations

from typing import Any


class HBaseConnectionConfig:
    """Configuration for HBase Thrift2 connection."""

    def __init__(
        self,
        host: str,
        port: int = 9090,
        timeout: int = 30000,
        ssl_options: dict[str, Any] | None = None,
        namespace: str = "default",
        retry_max_attempts: int = 3,
        retry_delay: float = 1.0,
        retry_backoff_factor: float = 2.0,
        use_http: bool = False,
    ):
        """Initialize connection configuration."""
        self.host = host
        self.port = port
        self.timeout = timeout
        self.ssl_options = ssl_options
        self.namespace = namespace
        self.retry_max_attempts = retry_max_attempts
        self.retry_delay = retry_delay
        self.retry_backoff_factor = retry_backoff_factor
        self.use_http = use_http


def create_connection_config(
    host: str,
    port: int = 9090,
    timeout: int = 30000,
    ssl_options: dict[str, Any] | None = None,
    namespace: str = "default",
    retry_max_attempts: int = 3,
    retry_delay: float = 1.0,
    retry_backoff_factor: float = 2.0,
    use_http: bool = False,
) -> HBaseConnectionConfig:
    """
    Create HBase connection configuration.

    Helper function to avoid code duplication.
    """
    return HBaseConnectionConfig(
        host=host,
        port=port,
        timeout=timeout,
        ssl_options=ssl_options,
        namespace=namespace,
        retry_max_attempts=retry_max_attempts,
        retry_delay=retry_delay,
        retry_backoff_factor=retry_backoff_factor,
        use_http=use_http,
    )
