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
from datetime import datetime, timedelta

from six.moves.urllib.request import Request

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.jenkins.hooks.jenkins import JenkinsHook
from airflow.providers.jenkins.operators.jenkins_job_trigger import JenkinsJobTriggerOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "concurrency": 8,
    "max_active_runs": 8,
}


with DAG(
    "test_jenkins", default_args=default_args, start_date=datetime(2017, 6, 1), schedule_interval=None
) as dag:
    job_trigger = JenkinsJobTriggerOperator(
        task_id="trigger_job",
        job_name="generate-merlin-config",
        parameters={"first_parameter": "a_value", "second_parameter": "18"},
        # parameters="resources/parameter.json", You can also pass a path to a json file containing your param
        jenkins_connection_id="your_jenkins_connection",  # T he connection must be configured first
    )

    def grab_artifact_from_jenkins(**context):
        """
        Grab an artifact from the previous job
        The python-jenkins library doesn't expose a method for that
        But it's totally possible to build manually the request for that
        """
        hook = JenkinsHook("your_jenkins_connection")
        jenkins_server = hook.get_jenkins_server()
        url = context['task_instance'].xcom_pull(task_ids='trigger_job')
        # The JenkinsJobTriggerOperator store the job url in the xcom variable corresponding to the task
        # You can then use it to access things or to get the job number
        # This url looks like : http://jenkins_url/job/job_name/job_number/
        url += "artifact/myartifact.xml"  # Or any other artifact name
        request = Request(url)
        response = jenkins_server.jenkins_open(request)
        return response  # We store the artifact content in a xcom variable for later use

    artifact_grabber = PythonOperator(task_id='artifact_grabber', python_callable=grab_artifact_from_jenkins)

    job_trigger >> artifact_grabber
