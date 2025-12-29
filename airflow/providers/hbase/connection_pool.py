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
"""HBase connection pool utilities."""

from __future__ import annotations

import threading
from typing import Dict, Any

import happybase

# Global pool storage
_pools: Dict[str, happybase.ConnectionPool] = {}
_pool_lock = threading.Lock()


def get_or_create_pool(conn_id: str, pool_size: int, **connection_args) -> happybase.ConnectionPool:
    """Get existing pool or create new one for connection ID.
    
    Args:
        conn_id: Connection ID
        pool_size: Pool size
        **connection_args: Arguments for happybase.Connection
        
    Returns:
        happybase.ConnectionPool instance
    """
    with _pool_lock:
        if conn_id not in _pools:
            _pools[conn_id] = happybase.ConnectionPool(pool_size, **connection_args)
        return _pools[conn_id]


def create_connection_pool(size: int, **connection_args) -> happybase.ConnectionPool:
    """Create HBase connection pool using happybase built-in pool.
    
    Args:
        size: Pool size
        **connection_args: Arguments for happybase.Connection
        
    Returns:
        happybase.ConnectionPool instance
    """
    return happybase.ConnectionPool(size, **connection_args)