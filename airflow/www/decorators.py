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

import functools
import gzip
import logging
from io import BytesIO as IO
from typing import Callable, TypeVar, cast

from flask import after_this_request, request

T = TypeVar("T", bound=Callable)

logger = logging.getLogger(__name__)


def action_logging(
    func: Callable | None = None,
    event: str | None = None,
    *,
    add_json_request_data_to_extra: bool = False,
) -> Callable[[T], T]:
    """Decorator to log user actions"""

    def log_action(f: T) -> T:
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            __tracebackhide__ = True  # Hide from pytest traceback.
            log_action(
                event=event or f.__name__, add_json_request_data_to_extra=add_json_request_data_to_extra
            )
            return f(*args, **kwargs)

        return cast(T, wrapper)

    if func:
        return log_action(func)
    return log_action


def gzipped(f: T) -> T:
    """Decorator to make a view compressed"""

    @functools.wraps(f)
    def view_func(*args, **kwargs):
        @after_this_request
        def zipper(response):
            accept_encoding = request.headers.get("Accept-Encoding", "")

            if "gzip" not in accept_encoding.lower():
                return response

            response.direct_passthrough = False

            if (
                response.status_code < 200
                or response.status_code >= 300
                or "Content-Encoding" in response.headers
            ):
                return response
            gzip_buffer = IO()
            gzip_file = gzip.GzipFile(mode="wb", fileobj=gzip_buffer)
            gzip_file.write(response.data)
            gzip_file.close()

            response.data = gzip_buffer.getvalue()
            response.headers["Content-Encoding"] = "gzip"
            response.headers["Vary"] = "Accept-Encoding"
            response.headers["Content-Length"] = len(response.data)

            return response

        return f(*args, **kwargs)

    return cast(T, view_func)
