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

import inspect
import json
from functools import wraps
from typing import Callable, TypeVar

import requests

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException, AirflowException
from airflow.settings import _ENABLE_AIP_44
from airflow.typing_compat import ParamSpec

PS = ParamSpec("PS")
RT = TypeVar("RT")


class InternalApiConfig:
    """Stores and caches configuration for Internal API."""

    _initialized = False
    _use_internal_api = False
    _internal_api_endpoint = ""

    @staticmethod
    def force_database_direct_access():
        """
        Block current component from using Internal API.

        All methods decorated with internal_api_call will always be executed locally.
        This mode is needed for "trusted" components like Scheduler, Webserver or Internal Api server.
        """
        InternalApiConfig._initialized = True
        InternalApiConfig._use_internal_api = False

    @staticmethod
    def get_use_internal_api():
        if not InternalApiConfig._initialized:
            InternalApiConfig._init_values()
        return InternalApiConfig._use_internal_api

    @staticmethod
    def get_internal_api_endpoint():
        if not InternalApiConfig._initialized:
            InternalApiConfig._init_values()
        return InternalApiConfig._internal_api_endpoint

    @staticmethod
    def _init_values():
        use_internal_api = conf.getboolean("core", "database_access_isolation", fallback=False)
        if use_internal_api and not _ENABLE_AIP_44:
            raise RuntimeError("The AIP_44 is not enabled so you cannot use it.")
        internal_api_endpoint = ""
        if use_internal_api:
            internal_api_url = conf.get("core", "internal_api_url")
            internal_api_endpoint = internal_api_url + "/internal_api/v1/rpcapi"
            if not internal_api_endpoint.startswith("http://"):
                raise AirflowConfigException("[core]internal_api_url must start with http://")

        InternalApiConfig._initialized = True
        InternalApiConfig._use_internal_api = use_internal_api
        InternalApiConfig._internal_api_endpoint = internal_api_endpoint


def internal_api_call(func: Callable[PS, RT]) -> Callable[PS, RT]:
    """
    Allow methods to be executed in database isolation mode.

    If [core]database_access_isolation is true then such method are not executed locally,
    but instead RPC call is made to Database API (aka Internal API). This makes some components
    decouple from direct Airflow database access.
    Each decorated method must be present in METHODS list in airflow.api_internal.endpoints.rpc_api_endpoint.
    Only static methods can be decorated. This decorator must be before "provide_session".

    See [AIP-44](https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-44+Airflow+Internal+API)
    for more information .
    """
    headers = {
        "Content-Type": "application/json",
    }

    def make_jsonrpc_request(method_name: str, params_json: str) -> bytes:
        data = {"jsonrpc": "2.0", "method": method_name, "params": params_json}
        internal_api_endpoint = InternalApiConfig.get_internal_api_endpoint()
        response = requests.post(url=internal_api_endpoint, data=json.dumps(data), headers=headers)
        if response.status_code != 200:
            raise AirflowException(
                f"Got {response.status_code}:{response.reason} when sending "
                f"the internal api request: {response.text}"
            )
        return response.content

    @wraps(func)
    def wrapper(*args, **kwargs):
        use_internal_api = InternalApiConfig.get_use_internal_api()
        if not use_internal_api:
            return func(*args, **kwargs)

        from airflow.serialization.serialized_objects import BaseSerialization  # avoid circular import

        bound = inspect.signature(func).bind(*args, **kwargs)
        arguments_dict = dict(bound.arguments)
        if "session" in arguments_dict:
            del arguments_dict["session"]
        if "cls" in arguments_dict:  # used by @classmethod
            del arguments_dict["cls"]

        args_json = json.dumps(
            BaseSerialization.serialize(arguments_dict, use_pydantic_models=True),
            default=BaseSerialization.serialize,
        )
        method_name = f"{func.__module__}.{func.__qualname__}"
        result = make_jsonrpc_request(method_name, args_json)
        if result is None or result == b"":
            return None
        return BaseSerialization.deserialize(json.loads(result), use_pydantic_models=True)

    return wrapper
