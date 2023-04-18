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
"""Serve logs process"""
from __future__ import annotations

import collections
import functools
import http.client
import http.server
import logging
import os
import socket

from jwt.exceptions import (
    ExpiredSignatureError,
    ImmatureSignatureError,
    InvalidAudienceError,
    InvalidIssuedAtError,
    InvalidSignatureError,
)
from setproctitle import setproctitle

from airflow.configuration import conf
from airflow.utils.docs import get_docs_url
from airflow.utils.jwt_signer import JWTSigner

logger = logging.getLogger(__name__)


class _Abort(Exception):
    code: http.HTTPStatus

    def __init__(self) -> None:
        super().__init__(self.code.description)


class _Forbidden(_Abort):
    code = http.HTTPStatus.FORBIDDEN


class _NotFound(_Abort):
    code = http.HTTPStatus.NOT_FOUND


class _JWTSignedFileRequestHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, signer: JWTSigner, directory: str) -> None:
        super().__init__(*args, directory=directory)
        self.signer = signer

    def translate_path(self, path: str) -> str:
        """Convert path for filesystem lookup.

        This is what SimpleHTTPRequestHandler uses to route the request to a
        file. We also perform auth here since it's the best place to get the
        URL path.
        """
        auth = self.headers.get("Authorization")
        if auth is None:
            logger.warning("The Authorization header is missing: %s.", self.headers)
            raise _Forbidden

        path = super().translate_path(path)
        if not path.startswith("/log/"):
            raise _NotFound
        request_filename = path[5:]

        try:
            payload = self.signer.verify_token(auth)
            token_filename = payload.get("filename")
        except InvalidAudienceError:
            logger.warning("Invalid audience for the request", exc_info=True)
            raise _Forbidden
        except InvalidSignatureError:
            logger.warning("The signature of the request was wrong", exc_info=True)
            raise _Forbidden
        except ImmatureSignatureError:
            logger.warning("The signature of the request was sent from the future", exc_info=True)
            raise _Forbidden
        except ExpiredSignatureError:
            logger.warning(
                "The signature of the request has expired. Make sure that all components "
                "in your system have synchronized clocks. "
                "See more at %s",
                get_docs_url("configurations-ref.html#secret-key"),
                exc_info=True,
            )
            raise _Forbidden
        except InvalidIssuedAtError:
            logger.warning(
                "The request was issues in the future. Make sure that all components "
                "in your system have synchronized clocks. "
                "See more at %s",
                get_docs_url("configurations-ref.html#secret-key"),
                exc_info=True,
            )
            raise _Forbidden
        except Exception:
            logger.warning("Unknown error", exc_info=True)
            raise _Forbidden

        if token_filename is None:
            logger.warning("The payload does not contain 'filename' key: %s.", payload)
            raise _Forbidden
        if token_filename != request_filename:
            logger.warning(
                "The requested log path is different than the one in token:"
                "Request path: %s. Token path: %s.",
                request_filename,
                token_filename,
            )
            raise _Forbidden

        return request_filename

    def guess_type(self, path):
        """Guess Content-Type to use in the response.

        This is simple since we only ever serves logs anyway.
        """
        return "application/json"

    def send_head(self):
        try:
            resp = super().send_head()
        except _Abort as e:
            self.send_error(e.code, str(e))
            resp = None
        return resp


def create_server(host, port) -> http.server.HTTPServer:
    handler_class = functools.partial(
        _JWTSignedFileRequestHandler,
        signer=JWTSigner(
            secret_key=conf.get("webserver", "secret_key"),
            expiration_time_in_seconds=conf.getint("webserver", "log_request_clock_grace", fallback=30),
            audience="task-instance-logs",
        ),
        directory=os.path.expanduser(conf.get("logging", "BASE_LOG_FOLDER")),
    )
    return http.server.ThreadingHTTPServer((host, port), handler_class)


GunicornOption = collections.namedtuple("GunicornOption", ["key", "value"])


def serve_logs(port: int | None = None) -> None:
    """Serves logs generated by Worker"""
    setproctitle("airflow serve-logs")

    # If dual stack is available and IPV6_V6ONLY is not enabled on the socket
    # then when IPV6 is bound to it will also bind to IPV4 automatically.
    if getattr(socket, "has_dualstack_ipv6", bool)():
        host = "[::]"
    else:
        host = "0.0.0.0"

    port = port or conf.getint("logging", "WORKER_LOG_SERVER_PORT")

    server = create_server(host, port)
    server.serve_forever()


if __name__ == "__main__":
    serve_logs()
