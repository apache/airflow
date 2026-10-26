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
"""Deterministic agent for the Databricks invocation system test."""

from __future__ import annotations

import asyncio
import os
from typing import Any

import uvicorn
from databricks_agentkit import DurableAgentServer, InvocationContext

app = DurableAgentServer()


@app.invoke
async def invoke(input: Any, context: InvocationContext) -> dict[str, Any]:
    # Keep the invocation running long enough to exercise background polling.
    await asyncio.sleep(2)
    return {
        "message": "Hello from Databricks",
        "received": input,
        "session_id": context.session_id,
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("DATABRICKS_APP_PORT", "8000")))
