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
from __future__ import annotations

from airflow_google_provider_resource_cleanup.handlers._base import BaseDeleteHandler
from airflow_google_provider_resource_cleanup.helpers import get_resource_path, run_command_async


async def delete_instance_group_manager(resource: dict, log_prefix: str):
    path = get_resource_path(resource)
    name = path.split("/")[-1]
    scope = "region" if "/regions/" in path else "zone"
    cmd = f"gcloud compute instance-groups managed delete {name} --{scope}={resource.get('location')} --quiet"
    return await run_command_async(cmd, log_prefix, check=True, ignore_not_found=True)


async def delete_instance(resource: dict, log_prefix: str):
    name = get_resource_path(resource).split("/")[-1]
    cmd = f"gcloud compute instances delete {name} --zone={resource['location']} --quiet"
    return await run_command_async(cmd, log_prefix, check=True, ignore_not_found=True)


async def delete_instance_template(resource: dict, log_prefix: str):
    path = get_resource_path(resource)
    name = path.split("/")[-1]
    scope = f"--region={resource['location']}" if "/regions/" in path else "--global"
    cmd = f"gcloud compute instance-templates delete {name} {scope} --quiet"
    return await run_command_async(cmd, log_prefix, check=True, ignore_not_found=True)


async def delete_disk(resource: dict, log_prefix: str):
    path = get_resource_path(resource)
    name = path.split("/")[-1]
    scope = "region" if "/regions/" in path else "zone"
    cmd = f"gcloud compute disks delete {name} --{scope}={resource['location']} --quiet"
    return await run_command_async(cmd, log_prefix, check=True, ignore_not_found=True)


async def delete_snapshot(resource: dict, log_prefix: str):
    name = get_resource_path(resource).split("/")[-1]
    cmd = f"gcloud compute snapshots delete {name} --quiet"
    return await run_command_async(cmd, log_prefix, check=True, ignore_not_found=True)


class ComputeDeleteHandler(BaseDeleteHandler):
    DELETERS = {
        "compute.googleapis.com/InstanceGroupManager": delete_instance_group_manager,
        "compute.googleapis.com/Instance": delete_instance,
        "compute.googleapis.com/InstanceTemplate": delete_instance_template,
        "compute.googleapis.com/Disk": delete_disk,
        "compute.googleapis.com/Snapshot": delete_snapshot,
    }

    DELETION_ORDER = [
        "compute.googleapis.com/InstanceGroupManager",
        "compute.googleapis.com/Instance",
        "compute.googleapis.com/InstanceTemplate",
        "compute.googleapis.com/Disk",
        "compute.googleapis.com/Snapshot",
    ]
