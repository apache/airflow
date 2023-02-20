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
import json
import logging
from typing import Any, Callable

from flask import Response

from airflow.api_connexion.types import APIResponse
from airflow.models import Variable, XCom
from airflow.models.dagwarning import DagWarning
from airflow.serialization.serialized_objects import BaseSerialization

log = logging.getLogger(__name__)


@functools.lru_cache()
def _initialize_map() -> dict[str, Callable]:
    from airflow.dag_processing.manager import DagFileProcessorManager
    from airflow.dag_processing.processor import DagFileProcessor
    from airflow.models.dag import DagModel

    functions: list[Callable] = [
        DagFileProcessor.update_import_errors,
        DagFileProcessor.manage_slas,
        DagFileProcessorManager.deactivate_stale_dags,
        DagModel.deactivate_deleted_dags,
        DagModel.get_paused_dag_ids,
        DagFileProcessorManager.clear_nonexistent_import_errors,
        DagWarning.purge_inactive_dag_warnings,
        XCom.get_value,
        XCom.get_one,
        XCom.get_many,
        XCom.clear,
        Variable.set,
        Variable.update,
        Variable.delete,
    ]
    return {f"{func.__module__}.{func.__qualname__}": func for func in functions}


def internal_airflow_api(body: dict[str, Any]) -> APIResponse:
    """Handler for Internal API /internal_api/v1/rpcapi endpoint."""
    log.debug("Got request")
    json_rpc = body.get("jsonrpc")
    if json_rpc != "2.0":
        log.error("Not jsonrpc-2.0 request.")
        return Response(response="Expected jsonrpc 2.0 request.", status=400)

    methods_map = _initialize_map()
    method_name = body.get("method")
    if method_name not in methods_map:
        log.error("Unrecognized method: %s.", method_name)
        return Response(response=f"Unrecognized method: {method_name}.", status=400)

    handler = methods_map[method_name]
    params = {}
    try:
        if body.get("params"):
            params_json = json.loads(str(body.get("params")))
            params = BaseSerialization.deserialize(params_json)
    except Exception as err:
        log.error("Error deserializing parameters.")
        log.error(err)
        return Response(response="Error deserializing parameters.", status=400)

    log.debug("Calling method %.", {method_name})
    try:
        output = handler(**params)
        output_json = BaseSerialization.serialize(output)
        log.debug("Returning response")
        return Response(
            response=json.dumps(output_json or "{}"), headers={"Content-Type": "application/json"}
        )
    except Exception as e:
        log.error("Error when calling method %s.", method_name)
        log.error(e)
        return Response(response=f"Error executing method: {method_name}.", status=500)
