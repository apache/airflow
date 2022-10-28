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

import base64
import inspect
import json
import requests
from airflow.exceptions import AirflowException
import pickle
from typing import List


# TODO read these from configuration
use_internal_api = True
url = "http://127.0.0.1:50051/internal/v1/rpcapi"


def remove_none(d: dict) -> dict:
    if isinstance(d, dict):
        for k, v in list(d.items()):
            if v is None:
                del d[k]
            else:
                remove_none(v)
    if isinstance(d, list):
        for v in d:
            remove_none(v)
    return d


def internal_api_call(
    method_name: str,
):
    headers = {
        "Content-Type": "application/json",
    }

    def jsonrpc_request(params_base64):

        data = {"jsonrpc": "2.0", "method": method_name, "params": params_base64}
        response = requests.post(url, data=json.dumps(data), headers=headers)
        if response.status_code != 200:
            print(f"Internal API error {response.content}")
            raise AirflowException(
                f"Got {response.status_code}:{response.reason} when submitting the internal api call."
            )
        return response.content

    def inner(func):
        def make_call(*args, **kwargs):

            if use_internal_api:
                print("internal_api_call")
                bound = inspect.signature(func).bind(*args, **kwargs)
                arguments_dict = dict(bound.arguments)
                if "session" in arguments_dict:
                    del arguments_dict["session"]
                args = base64.b64encode(pickle.dumps(arguments_dict)).decode("ascii")
                result = jsonrpc_request(args)
                if result is not None:
                    return pickle.loads(base64.b64decode(result))
                else:
                    return

            print("standard call")
            return func(*args, **kwargs)

        return make_call

    return inner


@internal_api_call("update_import_errors")
def test_me(dag_id: int, dag_run_id: int):
    print(f"dag_id: {dag_id}")
    print(f"dag_run_id: {dag_run_id}")
    return 15
