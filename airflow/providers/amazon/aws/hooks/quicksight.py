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

import time

from botocore.exceptions import ClientError

from airflow import AirflowException
from airflow.compat.functools import cached_property
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.sts import StsHook


class QuickSightHook(AwsBaseHook):
    """
    Interact with Amazon QuickSight.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.
    .. seealso::
    :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    NON_TERMINAL_STATES = {"INITIALIZED", "QUEUED", "RUNNING"}
    FAILED_STATES = {"FAILED"}

    def __init__(self, *args, **kwargs):
        super().__init__(client_type="quicksight", *args, **kwargs)

    @cached_property
    def sts_hook(self):
        return StsHook(aws_conn_id=self.aws_conn_id)

    def create_ingestion(
        self,
        data_set_id: str,
        ingestion_id: str,
        ingestion_type: str,
        wait_for_completion: bool = True,
        check_interval: int = 30,
    ) -> dict:
        """
        Creates and starts a new SPICE ingestion for a dataset. Refreshes the SPICE datasets

        :param data_set_id:  ID of the dataset used in the ingestion.
        :param ingestion_id: ID for the ingestion.
        :param ingestion_type: Type of ingestion . "INCREMENTAL_REFRESH"|"FULL_REFRESH"
        :param wait_for_completion: if the program should keep running until job finishes
        :param check_interval: the time interval in seconds which the operator
            will check the status of QuickSight Ingestion
        :return: Returns descriptive information about the created data ingestion
            having Ingestion ARN, HTTP status, ingestion ID and ingestion status.
        """
        self.log.info("Creating QuickSight Ingestion for data set id %s.", data_set_id)
        quicksight_client = self.get_conn()
        try:
            aws_account_id = self.sts_hook.get_account_number()
            create_ingestion_response = quicksight_client.create_ingestion(
                DataSetId=data_set_id,
                IngestionId=ingestion_id,
                IngestionType=ingestion_type,
                AwsAccountId=aws_account_id,
            )

            if wait_for_completion:
                self.wait_for_state(
                    aws_account_id=aws_account_id,
                    data_set_id=data_set_id,
                    ingestion_id=ingestion_id,
                    target_state={"COMPLETED"},
                    check_interval=check_interval,
                )
            return create_ingestion_response
        except Exception as general_error:
            self.log.error("Failed to run Amazon QuickSight create_ingestion API, error: %s", general_error)
            raise

    def get_status(self, aws_account_id: str, data_set_id: str, ingestion_id: str) -> str:
        """
        Get the current status of QuickSight Create Ingestion API.

        :param aws_account_id: An AWS Account ID
        :param data_set_id: QuickSight Data Set ID
        :param ingestion_id: QuickSight Ingestion ID
        :return: An QuickSight Ingestion Status
        """
        try:
            describe_ingestion_response = self.get_conn().describe_ingestion(
                AwsAccountId=aws_account_id, DataSetId=data_set_id, IngestionId=ingestion_id
            )
            return describe_ingestion_response["Ingestion"]["IngestionStatus"]
        except KeyError:
            raise AirflowException("Could not get status of the Amazon QuickSight Ingestion")
        except ClientError:
            raise AirflowException("AWS request failed, check logs for more info")

    def wait_for_state(
        self,
        aws_account_id: str,
        data_set_id: str,
        ingestion_id: str,
        target_state: set,
        check_interval: int,
    ):
        """
        Check status of a QuickSight Create Ingestion API

        :param aws_account_id: An AWS Account ID
        :param data_set_id: QuickSight Data Set ID
        :param ingestion_id: QuickSight Ingestion ID
        :param target_state: Describes the QuickSight Job's Target State
        :param check_interval: the time interval in seconds which the operator
            will check the status of QuickSight Ingestion
        :return: response of describe_ingestion call after Ingestion is is done
        """
        sec = 0
        status = self.get_status(aws_account_id, data_set_id, ingestion_id)
        while status in self.NON_TERMINAL_STATES and status != target_state:
            self.log.info("Current status is %s", status)
            time.sleep(check_interval)
            sec += check_interval
            if status in self.FAILED_STATES:
                raise AirflowException("The Amazon QuickSight Ingestion failed!")
            if status == "CANCELLED":
                raise AirflowException("The Amazon QuickSight SPICE ingestion cancelled!")
            status = self.get_status(aws_account_id, data_set_id, ingestion_id)

        self.log.info("QuickSight Ingestion completed")
        return status
