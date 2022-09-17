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

import os
from datetime import datetime

from requests import Request

from airflow import DAG
from airflow.decorators import task
from airflow.providers.jenkins.hooks.jenkins import JenkinsHook
from airflow.providers.jenkins.operators.jenkins_job_trigger import JenkinsJobTriggerOperator

JENKINS_CONNECTION_ID = "your_jenkins_connection"
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "test_jenkins"

with DAG(
    DAG_ID,
    default_args={
        "retries": 1,
        "concurrency": 8,
        "max_active_runs": 8,
    },
    start_date=datetime(2017, 6, 1),
    schedule=None,
) as dag:
    job_trigger = JenkinsJobTriggerOperator(
        task_id="trigger_job",
        job_name="generate-merlin-config",
        parameters={"first_parameter": "a_value", "second_parameter": "18"},
        # parameters="resources/parameter.json", You can also pass a path to a json file containing your param
        jenkins_connection_id=JENKINS_CONNECTION_ID,  # The connection must be configured first
    )

    @task
    def grab_artifact_from_jenkins(url):
        """
        Grab an artifact from the previous job
        The python-jenkins library doesn't expose a method for that
        But it's totally possible to build manually the request for that
        """
        hook = JenkinsHook(JENKINS_CONNECTION_ID)
        jenkins_server = hook.get_jenkins_server()
        # The JenkinsJobTriggerOperator store the job url in the xcom variable corresponding to the task
        # You can then use it to access things or to get the job number
        # This url looks like : http://jenkins_url/job/job_name/job_number/
        url += "artifact/myartifact.xml"  # Or any other artifact name
        request = Request(method='GET', url=url)
        response = jenkins_server.jenkins_open(request)
        return response  # We store the artifact content in a xcom variable for later use

    grab_artifact_from_jenkins(job_trigger.output)

    # Task dependency created via `XComArgs`:
    #   job_trigger >> grab_artifact_from_jenkins()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
