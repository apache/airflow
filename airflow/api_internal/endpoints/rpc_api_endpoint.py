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

import json
import logging

from flask import Response

from airflow.api_connexion.types import APIResponse
from airflow.dag_processing.processor import DagFileProcessor
from airflow.serialization.serialized_objects import BaseSerialization

log = logging.getLogger(__name__)

METHODS = {
    "dag_processing.processor.update_import_errors": DagFileProcessor.update_import_errors,
}


def json_rpc(
    body: dict,
) -> APIResponse:
    """Handler for Internal API /internal/v1/rpcapi endpoint."""
    log.debug("Got request")
    json_rpc = body.get("jsonrpc")
    if json_rpc != "2.0":
        log.warning("Not jsonrpc-2.0 request")
        return Response(response="Expected jsonrpc 2.0 request.", status=400)

    method_name = str(body.get("method"))
    if method_name not in METHODS:
        log.warning("Unrecognized method: %", method_name)
        return Response(response=f"Unrecognized method: {method_name}", status=400)

    params_json = body.get("params")
    if not params_json:
        params_json = "{}"
    handler = METHODS[method_name]
    try:
        params = BaseSerialization.deserialize(json.loads(params_json))
    except Exception as err:
        log.warning("Error deserializing parameters.")
        log.warning(err)
        return Response(response="Error deserializing parameters.", status=400)

    log.debug("Calling method %.", {method_name})
    handler = METHODS[method_name]
    output = handler(**params)
    if output:
        output_json = BaseSerialization.serialize(json.dumps(output))
    else:
        output_json = ""
    log.debug("Returning response")
    return Response(response=str(output_json), headers={"Content-Type": "application/json"})
