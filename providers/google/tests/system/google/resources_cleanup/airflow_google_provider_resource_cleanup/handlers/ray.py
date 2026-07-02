#
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
import asyncio

import vertex_ray

from airflow_google_provider_resource_cleanup.handlers._base import BaseDeleteHandler
from airflow_google_provider_resource_cleanup.handlers.ai import aiplatform_client

sdk_lock = asyncio.Lock()


async def delete_ray_cluster(resource, _):
    """Handler to delete all managed Ray clusters on Vertex AI."""
    # We dump outer prefix to the "_"variable, as we have to use the
    # custom one inside deleter function.

    async with sdk_lock:
        return await asyncio.to_thread(_sync_delete_ray_on_vertexai_clusters, resource)


def _sync_delete_ray_on_vertexai_clusters(resource):
    aiplatform_client(resource["project"], resource["location"])
    clusters = vertex_ray.list_ray_clusters()
    total_count = len(clusters)

    if total_count == 0:
        print(f"No Ray clusters found for {resource['project']} in {resource['location']}.")
        return False

    for counter, cluster in enumerate(clusters, 1):
        prefix = f"[{counter}/{total_count}]"
        try:
            vertex_ray.delete_ray_cluster(cluster.cluster_resource_name)
            print(f"{prefix} Deleted: {cluster.cluster_resource_name}")
        except Exception as e:
            print(f"Failed: {cluster.cluster_resource_name} - {e}")

    return True


class RayClusterOnVertexAIDeleteHandler(BaseDeleteHandler):
    DELETERS = {"vertex_ai_raycluster": delete_ray_cluster}

    DELETION_ORDER = ["vertex_ai_raycluster"]
