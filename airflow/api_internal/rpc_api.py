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

from typing import Callable
from flask import Response
import json
import logging

from airflow.callbacks.callback_requests import CallbackRequest, callback_from_dict

from typing import Any, Tuple, List
from airflow.api_connexion.types import APIResponse
from airflow.dag_processing.processor import DagFileProcessor

# internal_api_method : (module, class, method_name)
# PLEASE KEEP BACKWARD COMPATIBLE:
#   - do not remove/change 'internal_api_method' string.
#   - when adding more parameters (default values)


class InternalApiHandler:
    def __init__(
        self,
        func: Callable,
        args_to_json: Callable[[dict], str] = json.dumps,
        args_from_json: Callable[[str], dict] = lambda x: x,
        result_to_json: Callable[[Any], str] = json.dumps,
        result_from_json: Callable[[str], Any] = lambda x: x,
    ):
        self.func = func
        self.args_to_json = args_to_json
        self.args_from_json = args_from_json
        self.result_to_json = result_to_json
        self.result_from_json = result_from_json


def json_to_tuple(json_object: str) -> Tuple[Any, Any]:
    return tuple(json.loads(json_object))


def process_file_args_to_json(args: dict) -> dict:
    result_dict = args.copy()
    callback_requests: List[CallbackRequest] = args["callback_requests"]
    callbacks: List[dict] = []

    for callback in callback_requests:
        d = callback.to_dict().copy()
        d['type'] = str(callback.__class__.__name__)
        callbacks.append(d)
    result_dict["callback_requests"] = callbacks
    return result_dict


def process_file_args_from_json(args: dict) -> dict:
    result_dict = args.copy()
    callback_requests: List[dict] = result_dict["callback_requests"]
    callbacks: List[CallbackRequest] = []

    for callback in callback_requests:
        type_name = callback['type']
        del callback['type']
        callbacks.append(callback_from_dict(callback, type_name))
    result_dict["callback_requests"] = callbacks
    return result_dict


processor = DagFileProcessor(
    dag_ids=[], log=logging.getLogger('airflow'), dag_directory="/opt/airflow/airflow/example_dags"
)
METHODS = {
    'update_import_errors': InternalApiHandler(func=DagFileProcessor.update_import_errors),
    'process_file': InternalApiHandler(
        func=processor.process_file,
        args_to_json=process_file_args_to_json,
        args_from_json=process_file_args_from_json,
        result_from_json=json_to_tuple,
    ),
}

# handler for /internal/v1/rpcapi
def json_rpc(
    body: dict,
) -> APIResponse:
    """Process internal API request."""
    method_name = body.get("method")
    params = body.get("params")
    handler = METHODS[method_name]
    args = handler.args_from_json(params)
    output = handler.func(**args)
    if output is not None:
        output_json = handler.result_to_json(output)
    else:
        output_json = ''
    return Response(response=output_json, headers={"Content-Type": "application/json"})
