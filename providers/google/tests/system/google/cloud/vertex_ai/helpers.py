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

from google.api_core.exceptions import NotFound

from airflow.providers.google.cloud.hooks.vertex_ai.endpoint_service import EndpointServiceHook


def undeploy_models_and_delete_endpoint(project_id: str, region: str, endpoint_id: str) -> None:
    """Undeploy all models from a Vertex AI endpoint and delete it; do nothing if it does not exist."""
    endpoint_kwargs = {"project_id": project_id, "region": region, "endpoint": endpoint_id}
    hook = EndpointServiceHook()
    try:
        endpoint = hook.get_endpoint(**endpoint_kwargs)
    except NotFound:
        return
    # A model that still receives traffic can only be undeployed as the last one on the endpoint.
    deployed_models = sorted(
        endpoint.deployed_models, key=lambda model: endpoint.traffic_split.get(model.id, 0)
    )
    for deployed_model in deployed_models:
        hook.wait_for_operation(
            operation=hook.undeploy_model(**endpoint_kwargs, deployed_model_id=deployed_model.id)
        )
    hook.wait_for_operation(operation=hook.delete_endpoint(**endpoint_kwargs))
