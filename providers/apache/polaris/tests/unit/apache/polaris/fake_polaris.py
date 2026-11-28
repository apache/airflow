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
"""
A tiny in-process HTTP server standing in for Polaris.

Tests drive the real SDK clients and ``requests`` against it, so headers (bearer token, realm),
paths (including ``\\x1f``-encoded namespaces), bodies and retries are exercised end to end
without a Polaris deployment.
"""

from __future__ import annotations

import json
import re
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

TOKEN_PATH = "/api/catalog/v1/oauth/tokens"


@dataclass
class Request:
    method: str
    path: str
    query: dict[str, list[str]]
    headers: dict[str, str]
    body: bytes

    @property
    def json(self) -> Any:
        return json.loads(self.body) if self.body else None

    @property
    def form(self) -> dict[str, str]:
        return {k: v[0] for k, v in parse_qs(self.body.decode()).items()}


Responder = Callable[[Request], "tuple[int, Any]"]


@dataclass
class FakePolaris:
    """Route table plus a log of every request received."""

    url: str = ""
    routes: list[tuple[str, re.Pattern[str], Responder]] = field(default_factory=list)
    requests: list[Request] = field(default_factory=list)
    issued_tokens: int = 0

    def route(self, method: str, path_regex: str, responder: Responder | tuple[int, Any]) -> None:
        """Register a responder; later registrations win, so tests can override defaults."""
        handler = responder if callable(responder) else (lambda _req, r=responder: r)
        self.routes.insert(0, (method, re.compile(f"^{path_regex}$"), handler))

    def calls(self, method: str, path_regex: str) -> list[Request]:
        pattern = re.compile(f"^{path_regex}$")
        return [r for r in self.requests if r.method == method and pattern.match(r.path)]

    def token_responder(self, request: Request) -> tuple[int, Any]:
        self.issued_tokens += 1
        return 200, {"access_token": f"tok-{self.issued_tokens}", "token_type": "bearer", "expires_in": 3600}

    def dispatch(self, request: Request) -> tuple[int, Any]:
        self.requests.append(request)
        for method, pattern, handler in self.routes:
            if method == request.method and pattern.match(request.path):
                return handler(request)
        return (
            404,
            {
                "error": {
                    "message": f"no fake route for {request.method} {request.path}",
                    "type": "NoRoute",
                    "code": 404,
                }
            },
        )


@contextmanager
def serve() -> Iterator[FakePolaris]:
    """Run a fake Polaris on a free local port for the duration of the ``with`` block."""
    fake = FakePolaris()
    fake.route("POST", TOKEN_PATH, fake.token_responder)

    class Handler(BaseHTTPRequestHandler):
        def _handle(self):
            parts = urlsplit(self.path)
            length = int(self.headers.get("Content-Length") or 0)
            request = Request(
                method=self.command,
                path=unquote(parts.path),
                query=parse_qs(parts.query),
                headers={k.lower(): v for k, v in self.headers.items()},
                body=self.rfile.read(length) if length else b"",
            )
            status, body = fake.dispatch(request)
            payload = (
                b""
                if body is None or self.command == "HEAD"
                else (body if isinstance(body, bytes) else json.dumps(body).encode())
            )
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        do_GET = do_HEAD = do_POST = do_PUT = do_DELETE = _handle

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    fake.url = f"http://127.0.0.1:{server.server_port}"
    try:
        yield fake
    finally:
        server.shutdown()
        server.server_close()
