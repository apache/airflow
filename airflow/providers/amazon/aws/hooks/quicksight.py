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

import time
from typing import Optional

from botocore.exceptions import ClientError

from airflow import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class QuickSightHook(AwsBaseHook):
    """
    Interact with Amazon QuickSight.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.
    .. seealso::
    :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs):
        super().__init__(client_type="quicksight", *args, **kwargs)

    def create_ingestion(
        self,
        data_set_id: str,
        ingestion_id: str,
        aws_account_id: str,
        ingestion_type: str,
        wait_for_completion: bool = True,
        check_interval: int = 30,
        max_ingestion_time: Optional[int] = None,
    ):
        """
        Creates and starts a new SPICE ingestion for a dataset. Refreshes the SPICE datasets

        :param data_set_id:  ID of the dataset used in the ingestion.
        :param ingestion_id: ID for the ingestion.
        :param aws_account_id: Amazon Web Services account ID.
        :param ingestion_type: Type of ingestion . "INCREMENTAL_REFRESH"|"FULL_REFRESH"
        :param wait_for_completion: if the program should keep running until job finishes
        :param check_interval: the time interval in seconds which the operator
            will check the status of QuickSight Ingestion
        :param max_ingestion_time: the maximum ingestion time in seconds. If QuickSight ingestion runs
         longer than this will fail. Setting this to None implies no timeout for Ingestion.
         :return: Returns descriptive information about the created data ingestion
         having Ingestion ARN, HTTP status,
         ingestion ID and ingestion status.
        :rtype: Dict
        """
        self.log.info('Creating QuickSight Ingestion for data set id %s.', data_set_id)
        quicksight_client = self.get_conn()
        try:
            create_ingestion_response = quicksight_client.create_ingestion(
                DataSetId=data_set_id,
                IngestionId=ingestion_id,
                AwsAccountId=aws_account_id,
                IngestionType=ingestion_type,
            )
            if wait_for_completion:
                self.check_status(
                    aws_account_id=aws_account_id,
                    ingestion_id=ingestion_id,
                    data_set_id=data_set_id,
                    check_interval=check_interval,
                    max_ingestion_time=max_ingestion_time,
                )
            return create_ingestion_response
        except Exception as general_error:
            self.log.error("Failed to run QuickSight create_ingestion API, error: %s", general_error)
            raise

    def check_status(
        self,
        aws_account_id: str,
        ingestion_id: str,
        data_set_id: str,
        check_interval: int,
        max_ingestion_time: Optional[int] = None,
    ):
        """
        Check status of a QuickSight Create Ingestion API

        :param ingestion_id: name of the job to check status
        :param aws_account_id: the key of the response dict
            that points to the state
        :param data_set_id: the function used to retrieve the status
        :param check_interval: the time interval in seconds which the operator
            will check the status of QuickSight Ingestion
        :param max_ingestion_time: the maximum ingestion time in seconds. QuickSight API if
         run longer than this will fail. Setting this to None implies no timeout.
        :return: response of describe_ingestion call after Ingestion is is done
        """

        sec = 0
        running = True
        while running:
            time.sleep(check_interval)
            sec += check_interval
            try:
                describe_ingestion_response = self.get_conn().describe_ingestion(
                    AwsAccountId=aws_account_id, DataSetId=data_set_id, IngestionId=ingestion_id
                )
                status = describe_ingestion_response["Ingestion"]["IngestionStatus"]
                self.log.info("Job still running for %s seconds... current status is %s", sec, status)
            except KeyError:
                raise AirflowException("Could not get status of the QuickSight Ingestion")
            except ClientError:
                raise AirflowException("AWS request failed, check logs for more info")

            if status in {"INITIALIZED", "QUEUED", "RUNNING"}:
                running = True
            elif status == "FAILED":
                raise AirflowException(
                    f"QuickSight SPICE ingestion failed: "
                    f"{describe_ingestion_response['Ingestion']['ErrorInfo']}"
                )
            elif status == "CANCELLED":
                raise AirflowException("QuickSight SPICE ingestion cancelled")
            else:
                running = False

            if max_ingestion_time and sec > max_ingestion_time:
                # ensure that the job gets killed if the max ingestion time is exceeded
                raise AirflowException(f"QuickSight Ingestion took more than {max_ingestion_time} seconds")

        self.log.info("QuickSight Ingestion completed")
        return describe_ingestion_response
