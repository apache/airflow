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

from collections.abc import Iterator
from contextlib import contextmanager

import httpx2


def extract_error_detail(response: httpx2.Response) -> str:
    """Return the ``error`` field when it is a non-empty string, or the body cut to a short length."""
    try:
        body = response.json()
    except ValueError:
        body = None
    if isinstance(body, dict) and isinstance(error := body.get("error"), str) and error:
        return error
    return response.text.strip()[:200] or response.reason_phrase


@contextmanager
def translate_readiness_errors(
    *, server: str, root: str, timeout: float, failure: str, timeout_failure: str
) -> Iterator[None]:
    """
    Re-raise httpx2 errors from a readiness request with messages naming the failed step.

    :param server: Server name used to start each message, such as ``Ollama``.
    :param root: Server root, named when the server cannot be reached.
    :param timeout: Seconds reported in the timeout messages.
    :param failure: Message after *server* for an error status, followed by the status code and detail.
    :param timeout_failure: Message after *server* for a read timeout, followed by *timeout*.

    :raises httpx2.HTTPStatusError: On an error status, with the server's error text in the message.
    :raises TimeoutError: On a connect timeout or any other timeout.
    """
    try:
        yield
    except httpx2.HTTPStatusError as e:
        raise httpx2.HTTPStatusError(
            f"{server} {failure} ({e.response.status_code}): {extract_error_detail(e.response)}",
            request=e.request,
            response=e.response,
        ) from e
    except httpx2.ConnectTimeout as e:
        raise TimeoutError(f"Could not reach {server} at {root} within {timeout} seconds.") from e
    except httpx2.TimeoutException as e:
        raise TimeoutError(f"{server} {timeout_failure} within {timeout} seconds.") from e
