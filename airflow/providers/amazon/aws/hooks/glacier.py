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

from typing import Any

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class GlacierHook(AwsBaseHook):
    """
    Interact with Amazon Glacier.
    Provide thin wrapper around :external+boto3:py:class:`boto3.client("glacier") <Glacier.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, aws_conn_id: str = "aws_default") -> None:
        super().__init__(client_type="glacier")
        self.aws_conn_id = aws_conn_id

    def retrieve_inventory(self, vault_name: str) -> dict[str, Any]:
        """
        Initiate an Amazon Glacier inventory-retrieval job

        .. seealso::
            - :external+boto3:py:meth:`Glacier.Client.initiate_job`

        :param vault_name: the Glacier vault on which job is executed
        """
        job_params = {"Type": "inventory-retrieval"}
        self.log.info("Retrieving inventory for vault: %s", vault_name)
        response = self.get_conn().initiate_job(vaultName=vault_name, jobParameters=job_params)
        self.log.info("Initiated inventory-retrieval job for: %s", vault_name)
        self.log.info("Retrieval Job ID: %s", response["jobId"])
        return response

    def retrieve_inventory_results(self, vault_name: str, job_id: str) -> dict[str, Any]:
        """
        Retrieve the results of an Amazon Glacier inventory-retrieval job

        .. seealso::
            - :external+boto3:py:meth:`Glacier.Client.get_job_output`

        :param vault_name: the Glacier vault on which job is executed
        :param job_id: the job ID was returned by retrieve_inventory()
        """
        self.log.info("Retrieving the job results for vault: %s...", vault_name)
        response = self.get_conn().get_job_output(vaultName=vault_name, jobId=job_id)
        return response

    def describe_job(self, vault_name: str, job_id: str) -> dict[str, Any]:
        """
        Retrieve the status of an Amazon S3 Glacier job, such as an
        inventory-retrieval job

        .. seealso::
            - :external+boto3:py:meth:`Glacier.Client.describe_job`

        :param vault_name: the Glacier vault on which job is executed
        :param job_id: the job ID was returned by retrieve_inventory()
        """
        self.log.info("Retrieving status for vault: %s and job %s", vault_name, job_id)
        response = self.get_conn().describe_job(vaultName=vault_name, jobId=job_id)
        self.log.info("Job status: %s, code status: %s", response["Action"], response["StatusCode"])
        return response
