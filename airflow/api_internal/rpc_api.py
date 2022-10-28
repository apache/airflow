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

from flask import Response
import logging
import pickle
import base64
from airflow.api_connexion.types import APIResponse
from airflow.dag_processing.processor import DagFileProcessor

# internal_api_method : (module, class, method_name)
# PLEASE KEEP BACKWARD COMPATIBLE:
#   - do not remove/change 'internal_api_method' string.
#   - when adding more parameters (default values)


def test_me(dag_id: int, dag_run_id: int):
    print(f"dag_id: {dag_id}")
    print(f"dag_run_id: {dag_run_id}")
    return 11

processor = DagFileProcessor(
    dag_ids=[], log=logging.getLogger('airflow'), dag_directory="/opt/airflow/airflow/example_dags"
)
METHODS = {
    'update_import_errors': test_me,
    'process_file': processor.process_file
}

# handler for /internal/v1/rpcapi
def json_rpc(
    body: dict,
) -> APIResponse:
    """Process internal API request."""
    method_name = body.get("method")
    params = body.get("params")
    handler = METHODS[method_name]
    args = pickle.loads(base64.b64decode(params.encode('ascii')))
    output = handler(**args)
    if output is not None:
        output_base64 = base64.b64encode(pickle.dumps(output)).decode("ascii")
    else:
        output_base64 = ''
    return Response(response=output_base64, headers={"Content-Type": "text/plain"})
