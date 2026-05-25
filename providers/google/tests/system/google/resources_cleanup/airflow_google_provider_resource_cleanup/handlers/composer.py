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
import datetime

from airflow_google_provider_resource_cleanup.handlers._base import BaseDeleteHandler
from airflow_google_provider_resource_cleanup.helpers import get_resource_path, run_command_async

DAYS_PROTECTED = 2  # days of protection for composer env.
# e.g. if composer env was created more than 2 days before now it will be deleted else skipped


async def _delete_composer_environment(resource: dict, log_prefix: str):
    name = get_resource_path(resource)
    create_time = datetime.datetime.fromisoformat(resource["createTime"])
    age = datetime.datetime.now(datetime.timezone.utc) - create_time
    if not age > datetime.timedelta(days=DAYS_PROTECTED):
        print(f"Composer env with name: {name} was skipped because it is protected for {DAYS_PROTECTED} days.")
        return False
    cmd = f"gcloud composer environments delete {name} --location={resource['location']} --quiet"
    await run_command_async(cmd, log_prefix)


class ComposerDeleteHandler(BaseDeleteHandler):
    DELETERS = {
        "composer.googleapis.com/Environment": _delete_composer_environment,
    }

    DELETION_ORDER = [
        "composer.googleapis.com/Environment",
    ]
