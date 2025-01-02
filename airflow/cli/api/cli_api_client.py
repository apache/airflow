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

import contextlib
from functools import wraps
from typing import Callable, TypeVar, cast

import httpx

from airflow.cli.api.client import Client, Credentials
from airflow.typing_compat import ParamSpec

PS = ParamSpec("PS")
RT = TypeVar("RT")


@contextlib.contextmanager
def get_client():
    """Get CLI API client."""
    cli_api_client = None
    try:
        credentials = Credentials.load()
        limits = httpx.Limits(max_keepalive_connections=1, max_connections=10)
        cli_api_client = Client(
            base_url=credentials.api_url,
            limits=limits,
        )
        yield cli_api_client
    finally:
        if cli_api_client:
            cli_api_client.close()


def provide_cli_api_client(func: Callable[PS, RT]) -> Callable[PS, RT]:
    """
    Provide a CLI API Client to the decorated function.

    CLI API Client shouldn't be passed to the function when this wrapper is used
    if the purpose is not mocking or testing.
    If you want to reuse a CLI API Client or run the function as part of
    API call, you pass it to the function, if not this wrapper
    will create one and close it for you.
    """

    @wraps(func)
    def wrapper(*args, **kwargs) -> RT:
        if "cli_api_client" not in kwargs:
            with get_client() as cli_api_client:
                return func(*args, cli_api_client=cli_api_client, **kwargs)
        # The CLI API Client should be only passed for Mocking and Testing
        return func(*args, **kwargs)

    return wrapper


NEW_CLI_API_CLIENT: Client = cast(Client, None)
