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

import urllib.parse

from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator
from airflow.providers.sas._utils.logon import create_session_for_connection

JES_URI = "/jobExecution"
JOB_URI = f"{JES_URI}/jobs"


class SASJobExecutionOperator(BaseOperator):
    """
    Executes a SAS Job

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SASJobExecutionOperator`

    :param connection_name: Name of the SAS Viya connection stored as an Airflow HTTP connection
    :param job_name: Name of the SAS Job to be run
    :param parameters Dictionary of all the parameters that should be passed to the
        SAS Job as SAS Macro variables
    """

    def __init__(self, connection_name: str, job_name: str, parameters: dict, **kwargs) -> None:
        super().__init__(**kwargs)
        self.connection_name = connection_name
        self.job_name = job_name
        self.parameters = parameters

    def execute(self, context):
        session = create_session_for_connection(self.connection_name)

        print(f"Executing SAS job: {self.job_name}")
        # url escape the program name
        program_name = urllib.parse.quote(self.job_name)
        url_string = ""
        for key, value in self.parameters.items():
            url_string += f"&{key}={urllib.parse.quote(value)}"

        url = f"/SASJobExecution/?_program={program_name}{url_string}"

        payload = {
            "_program": self.job_name,
            "_action": "wait,execute",
            "_output_type": "html",
            "_debug": "log",
        }

        headers = {"Accept": "application/vnd.sas.job.execution.job+json"}
        response = session.post(url, headers=headers, data=payload, verify=False)

        if response.status_code != 200:
            raise AirflowFailException(f"SAS Job Execution HTTP status code {response.status_code}")

        return 1
