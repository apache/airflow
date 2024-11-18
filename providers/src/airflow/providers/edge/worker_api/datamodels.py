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

from typing import Any, Dict, Optional

from pydantic import BaseModel


class JsonRpcRequest(BaseModel):
    """JSON RPC request model."""

    method: str
    """Fully qualified python module method name that is called via JSON RPC."""
    jsonrpc: str
    """JSON RPC version."""
    params: Optional[Dict[str, Any]] = None  # noqa: UP006, UP007 - prevent pytest failing in back-compat
    """Parameters passed to the method."""
