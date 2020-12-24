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

from cached_property import cached_property

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class AwsGlueCrawlerHook(AwsBaseHook):
    """
    Interacts with AWS Glue Crawler
    :param poll_interval: Time (in seconds) to wait between two consecutive calls to check crawler status
    :type poll_interval: int
    :param config = Configurations for the AWS Glue crawler
    :type config = Optional[dict]
    """

    def __init__(self, poll_interval, *args, **kwargs):
        self.poll_interval = poll_interval
        kwargs['client_type'] = 'glue'
        super().__init__(*args, **kwargs)

    @cached_property
    def glue_client(self):
        """:return: AWS Glue client"""
        return self.get_conn()

    def get_iam_execution_role(self, role_name) -> str:
        """:return: iam role for crawler execution"""
        iam_client = self.get_client_type('iam', self.region_name)

        try:
            glue_execution_role = iam_client.get_role(RoleName=role_name)
            self.log.info("Iam Role Name: %s", role_name)
            return glue_execution_role
        except Exception as general_error:
            self.log.error("Failed to create aws glue crawler, error: %s", general_error)
            raise

    def get_or_create_crawler(self, config) -> str:
        """
        Creates the crawler if the crawler doesn't exists and returns the crawler name

        :param config = Configurations for the AWS Glue crawler
        :type config = Optional[dict]
        :return: Name of the crawler
        """
        self.get_iam_execution_role(config["Role"])

        try:
            self.glue_client.get_crawler(**config)
            self.log.info("Crawler already exists")
            try:
                self.glue_client.update_crawler(**config)
                return config["Name"]
            except Exception as general_error:
                self.log.error("Failed to update aws glue crawler, error: %s", general_error)
                raise
        except self.glue_client.exceptions.EntityNotFoundException:
            self.log.info("Creating AWS Glue crawler")
            try:
                self.glue_client.create_crawler(**config)
                return config["Name"]
            except Exception as general_error:
                self.log.error("Failed to create aws glue crawler, error: %s", general_error)
                raise

    def start_crawler(self, crawler_name: str) -> str:
        """
        Triggers the AWS Glue crawler
        :return: Empty dictionary
        """
        try:
            crawler_run = self.glue_client.start_crawler(Name=crawler_name)
            return crawler_run
        except Exception as general_error:
            self.log.error("Failed to run aws glue crawler, error: %s", general_error)
            raise

    def get_crawler_state(self, crawler_name: str) -> str:
        """
        Get state of the Glue crawler. The crawler state can be
        ready, running, or stopping.
        :param crawler_name: unique crawler name per AWS account
        :type crawler_name: str
        :return: State of the Glue crawler
        """
        crawler_run = self.glue_client.get_crawler(Name=crawler_name)
        crawler_run_state = crawler_run['Crawler']['State']
        return crawler_run_state

    def get_last_crawl_status(self, crawler_name: str) -> str:
        """
        Get the status of the latest crawl run. The crawl
        status can be succeeded, cancelled, or failed.
        :param crawler_name: unique crawler name per AWS account
        :type crawler_name: str
        :return: Status of the Glue crawler
        """
        crawler = self.glue_client.get_crawler(Name=crawler_name)
        last_crawl_status = crawler['Crawler']['LastCrawl']['Status']
        return last_crawl_status

    def wait_for_crawler_completion(self, crawler_name: str) -> str:
        """
        Waits until Glue crawler completes or
        fails and returns the final state if finished.
        Raises AirflowException when the crawler failed
        :param crawler_name: unique crawler name per AWS account
        :type crawler_name: str
        :return: Dict of crawler's status
        """
        failed_status = ['FAILED', 'CANCELLED']

        while True:
            crawler_run_state = self.get_crawler_state(crawler_name)
            if crawler_run_state == 'READY':
                self.log.info("Crawler: %s State: %s", crawler_name, crawler_run_state)
                crawler_run_status = self.get_last_crawl_status(crawler_name)
                if crawler_run_status in failed_status:
                    crawler_error_message = (
                        "Exiting Crawler: " + crawler_name + " Run State: " + crawler_run_state
                    )
                    self.log.info(crawler_error_message)
                    raise AirflowException(crawler_error_message)
                else:
                    self.log.info("Crawler Status: %s", crawler_run_status)
                    metrics = self.get_crawler_metrics(crawler_name)
                    print('Last Runtime Duration (seconds): ', metrics['LastRuntimeSeconds'])
                    print('Median Runtime Duration (seconds): ', metrics['MedianRuntimeSeconds'])
                    print('Tables Created: ', metrics['TablesCreated'])
                    print('Tables Updated: ', metrics['TablesUpdated'])
                    print('Tables Deleted: ', metrics['TablesDeleted'])

                    return crawler_run_status

            else:
                self.log.info(
                    "Polling for AWS Glue crawler: %s Current run state: %s",
                    crawler_name,
                    crawler_run_state,
                )
                time.sleep(self.poll_interval)

                metrics = self.get_crawler_metrics(crawler_name)
                time_left = int(metrics['TimeLeftSeconds'])

                if time_left > 0:
                    print('Estimated Time Left (seconds): ', time_left)
                    self.poll_interval = time_left
                else:
                    print('Crawler should finish soon')

    def get_crawler_metrics(self, crawler_name: str) -> str:
        """
        Returns metrics associated with the crawler
        :return: Dictionary of the crawler metrics
        """
        crawler = self.glue_client.get_crawler_metrics(CrawlerNameList=[crawler_name])

        metrics = crawler['CrawlerMetricsList'][0]

        return metrics
