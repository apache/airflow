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

from time import sleep

from cached_property import cached_property

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class AwsGlueCrawlerHook(AwsBaseHook):
    """
    Interacts with AWS Glue Crawler
    :param config = Configurations for the AWS Glue crawler
    :type config = dict
    """

    def __init__(self, *args, **kwargs):
        kwargs['client_type'] = 'glue'
        super().__init__(*args, **kwargs)

    @cached_property
    def glue_client(self):
        """:return: AWS Glue client"""
        return self.get_conn()

    def check_iam_role(self, role_name: str) -> str:
        """
        Checks if the input IAM role name is a
        valid pre-existing role within the caller's AWS account.
        Is needed because the current Boto3 (<=1.16.46)
        glue client create_crawler() method misleadingly catches
        non-existing role as a role trust policy error.
        :param role_name = IAM role name
        :type role_name = str
        :return: IAM role name
        """
        iam_client = self.get_client_type('iam', self.region_name)

        iam_client.get_role(RoleName=role_name)

    def get_or_create_crawler(self, **crawler_kwargs) -> str:
        """
        Creates the crawler if the crawler doesn't exists and returns the crawler name

        :param crawler_kwargs = Keyword args that define the configurations used to create/update the crawler
        :type crawler_kwargs = any
        :return: Name of the crawler
        """
        crawler_name = crawler_kwargs['Name']
        try:
            glue_response = self.glue_client.get_crawler(Name=crawler_name)
            self.log.info("Crawler %s already exists; updating crawler", crawler_name)
            self.glue_client.update_crawler(**crawler_kwargs)
            return glue_response['Crawler']['Name']
        except self.glue_client.exceptions.EntityNotFoundException:
            self.log.info("Creating AWS Glue crawler %s", crawler_name)
            try:
                glue_response = self.glue_client.create_crawler(**crawler_kwargs)
                return glue_response['Crawler']['Name']
            except self.glue_client.exceptions.InvalidInputException as general_error:
                self.check_iam_role(crawler_kwargs['Role'])
                raise AirflowException(general_error)

    def start_crawler(self, crawler_name: str) -> dict:
        """
        Triggers the AWS Glue crawler
        :return: Empty dictionary
        """
        crawler = self.glue_client.start_crawler(Name=crawler_name)
        return crawler

    def get_crawler_state(self, crawler_name: str) -> str:
        """
        Get state of the Glue crawler. The crawler state can be
        ready, running, or stopping.
        :param crawler_name: unique crawler name per AWS account
        :type crawler_name: str
        :return: State of the Glue crawler
        """
        crawler = self.glue_client.get_crawler(Name=crawler_name)
        crawler_state = crawler['Crawler']['State']
        return crawler_state

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

    def wait_for_crawler_completion(self, crawler_name: str, poll_interval: int = 5) -> str:
        """
        Waits until Glue crawler completes and
        returns the status of the latest crawl run.
        Raises AirflowException if the crawler fails or is cancelled.
        :param crawler_name: unique crawler name per AWS account
        :type crawler_name: str
        :param poll_interval: Time (in seconds) to wait between two consecutive calls to check crawler status
        :type poll_interval: int
        :return: Crawler's status
        """
        failed_status = ['FAILED', 'CANCELLED']

        while True:
            crawler_state = self.get_crawler_state(crawler_name)
            if crawler_state == 'READY':
                self.log.info("State: %s", crawler_state)
                crawler_status = self.get_last_crawl_status(crawler_name)
                if crawler_status in failed_status:
                    raise AirflowException(
                        f"Status: {crawler_status}"
                    )  # pylint: disable=raising-format-tuple
                else:
                    metrics = self.get_crawler_metrics(crawler_name)
                    self.log.info("Status: %s", crawler_status)
                    self.log.info("Last Runtime Duration (seconds): %s", metrics['LastRuntimeSeconds'])
                    self.log.info("Median Runtime Duration (seconds): %s", metrics['MedianRuntimeSeconds'])
                    self.log.info("Tables Created: %s", metrics['TablesCreated'])
                    self.log.info("Tables Updated: %s", metrics['TablesUpdated'])
                    self.log.info("Tables Deleted: %s", metrics['TablesDeleted'])

                    return crawler_status

            else:
                self.log.info("Polling for AWS Glue crawler: %s ", crawler_name)
                self.log.info("State: %s", crawler_state)

                metrics = self.get_crawler_metrics(crawler_name)
                time_left = int(metrics['TimeLeftSeconds'])

                if time_left > 0:
                    self.log.info("Estimated Time Left (seconds): %s", time_left)
                else:
                    self.log.info("Crawler should finish soon")

                sleep(poll_interval)

    def get_crawler_metrics(self, crawler_name: str) -> dict:
        """
        Returns metrics associated with the crawler
        :return: Dictionary of the crawler metrics
        """
        crawler = self.glue_client.get_crawler_metrics(CrawlerNameList=[crawler_name])

        metrics = crawler['CrawlerMetricsList'][0]

        return metrics
