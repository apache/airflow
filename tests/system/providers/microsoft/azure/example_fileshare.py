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
import os
from datetime import datetime

from airflow.decorators import task
from airflow.models import DAG
from airflow.providers.microsoft.azure.hooks.fileshare import AzureFileShareHook

NAME = 'myfileshare'
DIRECTORY = "mydirectory"
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_fileshare"


@task
def create_fileshare():
    """Create a fileshare with directory"""
    hook = AzureFileShareHook()
    hook.create_share(NAME)
    hook.create_directory(share_name=NAME, directory_name=DIRECTORY)
    exists = hook.check_for_directory(share_name=NAME, directory_name=DIRECTORY)
    if not exists:
        raise Exception


@task
def delete_fileshare():
    """Delete a fileshare"""
    hook = AzureFileShareHook()
    hook.delete_share(NAME)


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    create_fileshare() >> delete_fileshare()

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
