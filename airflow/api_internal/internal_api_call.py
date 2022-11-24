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
from typing import Callable, TypeVar

import requests

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.serialization.serialized_objects import BaseSerialization
from airflow.typing_compat import ParamSpec

PS = ParamSpec("PS")
RT = TypeVar("RT")

_use_internal_api = conf.get("core", "database_access_isolation")
_internal_api_url = conf.get("core", "database_api_url")

_internal_api_endpoint = _internal_api_url + "/internal/v1/rpcapi"
if not _internal_api_endpoint.startswith("http://"):
    _internal_api_endpoint = "http://" + _internal_api_endpoint


def internal_api_call(method_name: str):
    """Decorator for methods which may be executed in database isolation mode.

    If [core]database_access_isolation is true then such method are not executed locally,
    but instead RPC call is made to Database API (aka Internal API). This makes some components
    stop depending on Airflow database access.
    Each decorated method must be present in METHODS list in airflow.api_internal.endpoints.rpc_api_endpoint.
    Only static methods can be decorated. This decorator must be before "provide_session".

    See AIP-44 for more information.
    """
    headers = {
        "Content-Type": "application/json",
    }

    def make_jsonrpc_request(params_json: str) -> bytes:
        data = {"jsonrpc": "2.0", "method": method_name, "params": params_json}
        response = requests.post(_internal_api_endpoint, data=json.dumps(data), headers=headers)
        if response.status_code != 200:
            raise AirflowException(
                f"Got {response.status_code}:{response.reason} when sending the internal api request."
            )
        return response.content

    def inner(func: Callable[PS, RT | None]) -> Callable[PS, RT | None]:
        def make_call(*args, **kwargs) -> RT | None:
            if not _use_internal_api:
                return func(*args, **kwargs)

            bound = inspect.signature(func).bind(*args, **kwargs)
            arguments_dict = dict(bound.arguments)
            if "session" in arguments_dict:
                del arguments_dict["session"]
            args_json = json.dumps(BaseSerialization.serialize(arguments_dict))
            result = make_jsonrpc_request(args_json)
            if result:
                return BaseSerialization.deserialize(json.loads(result))
            else:
                return None

        return make_call

    return inner
