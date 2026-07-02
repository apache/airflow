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


async def delete_instance_group_manager(resource):
    name = get_resource_path(resource)
    cmd = f"gcloud compute instance-groups managed delete {name}"
    await run_command_async(cmd)


async def delete_disk(resource):
    name = get_resource_path(resource)
    if resource.get("additionalAttributes") and resource["additionalAttributes"].get("users"):
        print(
            f"compute.googleapis.com/Disk with name: {name} was skipped "
            f"because it is still used by: {resource['additionalAttributes']['users']}"
        )
        return False
    cmd = f"gcloud compute disks delete {name} --zone={resource['location']} --quiet"
    await run_command_async(cmd)


class ComputeDeleteHandler(BaseDeleteHandler):
    DELETERS = {
        "compute.googleapis.com/InstanceGroupManager": delete_instance_group_manager,
        "compute.googleapis.com/Disk": delete_disk,
    }

    DELETION_ORDER = [
        "compute.googleapis.com/InstanceGroupManager",
        "compute.googleapis.com/Disk",
    ]
