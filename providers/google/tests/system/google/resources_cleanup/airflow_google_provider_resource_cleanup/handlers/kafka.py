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
from airflow_google_provider_resource_cleanup.handlers._base import BaseDeleteHandler
from airflow_google_provider_resource_cleanup.helpers import get_resource_path, run_command_async


def __extract_params(resource: dict) -> tuple[str, str]:
    """
    Extract the location and name of the resource.
    """
    region = resource.get("location")
    if not isinstance(region, str):
        raise TypeError("Managed Kafka resource location must be a string.")
    name = get_resource_path(resource)
    return name, region


async def _delete_managed_kafka_cluster(resource: dict, log_prefix: str):
    name, location = __extract_params(resource)
    cmd = f"gcloud managed-kafka clusters delete {name} --location={location} --quiet"
    await run_command_async(cmd, log_prefix)


class ManagedKafkaDeleteHandler(BaseDeleteHandler):
    DELETERS = {"managedkafka.googleapis.com/Cluster": _delete_managed_kafka_cluster}

    DELETION_ORDER = [
        "managedkafka.googleapis.com/Cluster",
    ]
