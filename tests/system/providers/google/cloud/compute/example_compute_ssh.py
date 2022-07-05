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

"""
Example Airflow DAG that starts, stops and sets the machine type of a Google Compute
Engine instance.

"""

import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator

# [START howto_operator_gce_args_common]
ENV_ID = os.environ.get('SYSTEM_TESTS_ENV_ID')
PROJECT_ID = os.environ.get('SYSTEM_TESTS_GCP_PROJECT')

GCE_INSTANCE = 'compute-ssh-test'

DAG_ID = 'cloud_compute_ssh'
LOCATION = 'europe-west2-a'
# [END howto_operator_gce_args_common]

with models.DAG(
    DAG_ID,
    schedule_interval='@once',  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    # [START howto_execute_command_on_remote1]
    # todo: witch credentials can i use
    # todo: error SSH operator error: Authentication failed. in airflow/providers/ssh/operators/ssh.py
    os_login_without_iap_tunnel = SSHOperator(
        task_id="os_login_without_iap_tunnel",
        ssh_hook=ComputeEngineSSHHook(
            instance_name=GCE_INSTANCE,
            zone=LOCATION,
            project_id=PROJECT_ID,
            use_oslogin=True,
            use_iap_tunnel=False,
        ),
        command="echo os_login_without_iap_tunnel",
    )
    # [END howto_execute_command_on_remote1]

    # # [START howto_execute_command_on_remote2]
    # metadata_without_iap_tunnel = SSHOperator(
    #     task_id="metadata_without_iap_tunnel",
    #     ssh_hook=ComputeEngineSSHHook(
    #         instance_name=GCE_INSTANCE,
    #         zone=LOCATION,
    #         use_oslogin=False,
    #         use_iap_tunnel=False,
    #     ),
    #     command="echo metadata_without_iap_tunnel",
    # )
    # # [END howto_execute_command_on_remote2]
    #
    # os_login_with_iap_tunnel = SSHOperator(
    #     task_id="os_login_with_iap_tunnel",
    #     ssh_hook=ComputeEngineSSHHook(
    #         instance_name=GCE_INSTANCE,
    #         zone=LOCATION,
    #         use_oslogin=True,
    #         use_iap_tunnel=True,
    #     ),
    #     command="echo os_login_with_iap_tunnel",
    # )
    #
    # metadata_with_iap_tunnel = SSHOperator(
    #     task_id="metadata_with_iap_tunnel",
    #     ssh_hook=ComputeEngineSSHHook(
    #         instance_name=GCE_INSTANCE,
    #         zone=LOCATION,
    #         use_oslogin=False,
    #         use_iap_tunnel=True,
    #     ),
    #     command="echo metadata_with_iap_tunnel",
    # )

    os_login_without_iap_tunnel
    # os_login_with_iap_tunnel >> os_login_without_iap_tunnel
    # metadata_with_iap_tunnel >> metadata_without_iap_tunnel

    # os_login_without_iap_tunnel >> metadata_with_iap_tunnel

    # ### Everything below this line is not part of example ###
    # ### Just for system tests purpose ###
    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
