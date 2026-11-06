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
"""Minimal agent served from the custom container used by the Agent Engine system test."""

from __future__ import annotations

import contextlib
import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any


class _InvocationHandler(BaseHTTPRequestHandler):
    """Serve health checks and query requests for the Agent Engine system test."""

    def do_GET(self) -> None:
        self._send_json(200, {"ok": True})

    def do_POST(self) -> None:
        input_value = None
        with contextlib.suppress(Exception):
            length = int(self.headers.get("content-length", "0"))
            payload = json.loads(self.rfile.read(length) or b"{}")
            input_value = payload.get("input", payload)
            if isinstance(input_value, str):
                input_value = json.loads(input_value)
        self._send_json(200, {"output": {"message": "Hello from Agent Engine", "received": input_value}})

    def _send_json(self, status: int, body: dict[str, Any]) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    HTTPServer(("0.0.0.0", port), _InvocationHandler).serve_forever()
