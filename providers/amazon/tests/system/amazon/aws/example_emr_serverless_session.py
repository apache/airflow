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

from datetime import datetime

from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessDeleteApplicationOperator,
    EmrServerlessGetSessionEndpointOperator,
    EmrServerlessStartSessionOperator,
    EmrServerlessStopSessionOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrServerlessSessionSensor
from airflow.providers.common.compat.sdk import DAG, TriggerRule, chain

from system.amazon.aws.utils import SystemTestContextBuilder

DAG_ID = "example_emr_serverless_session"

# Externally fetched variables:
ROLE_ARN_KEY = "ROLE_ARN"

sys_test_context_task = SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).build()

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "emr-serverless"],
) as dag:
    test_context = sys_test_context_task()
    role_arn = test_context[ROLE_ARN_KEY]

    create_app = EmrServerlessCreateApplicationOperator(
        task_id="create_app",
        release_label="emr-7.13.0",
        job_type="SPARK",
        config={
            "name": "session-systest",
            # Interactive sessions (required for the StartSession API) are only available
            # on emr-7.13.0+ and must be explicitly enabled on the application.
            "interactiveConfiguration": {"sessionEnabled": True},
        },
    )
    application_id = create_app.output

    # [START howto_operator_emr_serverless_start_session]
    start_session = EmrServerlessStartSessionOperator(
        task_id="start_session",
        application_id=application_id,
        execution_role_arn=role_arn,
        idle_timeout_minutes=5,
    )
    # [END howto_operator_emr_serverless_start_session]
    session_id = start_session.output["session_id"]

    # [START howto_sensor_emr_serverless_session]
    wait_for_session = EmrServerlessSessionSensor(
        task_id="wait_for_session",
        application_id=application_id,
        session_id=session_id,
    )
    # [END howto_sensor_emr_serverless_session]

    # [START howto_operator_emr_serverless_get_session_endpoint]
    get_endpoint = EmrServerlessGetSessionEndpointOperator(
        task_id="get_endpoint",
        application_id=application_id,
        session_id=session_id,
    )
    # [END howto_operator_emr_serverless_get_session_endpoint]

    # [START howto_operator_emr_serverless_stop_session]
    stop_session = EmrServerlessStopSessionOperator(
        task_id="stop_session",
        application_id=application_id,
        session_id=session_id,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_emr_serverless_stop_session]

    delete_app = EmrServerlessDeleteApplicationOperator(
        task_id="delete_app",
        application_id=application_id,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        # TEST SETUP
        test_context,
        create_app,
        # TEST BODY
        start_session,
        wait_for_session,
        get_endpoint,
        # TEST TEARDOWN
        stop_session,
        delete_app,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
