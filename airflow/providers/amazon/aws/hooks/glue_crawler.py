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
from typing import Dict, List

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class AwsGlueCrawlerHook(AwsBaseHook):
    """
    Interacts with AWS Glue Crawler
    :param crawler_name = Unique crawler name per AWS Account
    :type crawler_name = Optional[str]
    :param crawler_desc = Crawler description
    :type crawler_desc = Optional[str]
    :param db_name = AWS glue catalog database ID
    :type db_name = Optional[str]
    :param iam_role_name = AWS IAM role for glue crawler
    :type iam_role_name = Optional[str]
    :param region_name = AWS region name (e.g. 'us-west-2')
    :type region_name = Optional[str]
    :param targets = Target sources to be crawled
    :type targets = Optional[dict]
    :param classifiers = List of user defined custom classifiers to be used by the crawler
    :type classifiers = Optional[list]
    :param table_prefix = Prefix for catalog table to be created
    :type table_prefix = Optional[str]
    :param schema_change_policy = Behavior when the crawler discovers an update or deletion
    :type schema_change_policy = Optional[dict]
    :param recrawl_policy = Behavior when the crawler needs to crawl again
    :type recrawl_policy = Optional[dict]
    :param lineage_settings = Enables or disables data lineage
    :type lineage_settings = Optional[str]
    :param configuration = Versioned JSON configuration specifying additional crawler configurations
    :type configuration = Optional[str]
    :param security_configuration = Name of the security configuration structure to be used by the crawler.
    :type security_configuration = Optional[str]
    :param tags = Tags to attach to the crawler
    :type tags = Optional[dict]
    """

    CRAWLER_POLL_INTERVAL = 6  # polls crawler status after every CRAWLER_POLL_INTERVAL seconds

    def __init__(
        self,
        crawler_name=None,
        crawler_desc=None,
        db_name=None,
        iam_role_name=None,
        targets=None,
        classifiers=None,
        table_prefix=None,
        schema_change_policy=None,
        recrawl_policy=None,
        lineage_settings=None,
        configuration=None,
        security_configuration=None,
        tags=None,
        *args,
        **kwargs,
    ):

        self.crawler_name = crawler_name
        self.crawler_desc = crawler_desc
        self.db_name = db_name
        self.iam_role_name = iam_role_name
        self.targets = targets
        self.classifiers = classifiers
        self.table_prefix = table_prefix
        self.schema_change_policy = schema_change_policy
        self.recrawl_policy = recrawl_policy
        self.lineage_settings = lineage_settings
        self.configuration = configuration
        self.security_configuration = security_configuration
        self.tags = tags
        kwargs['client_type'] = 'glue'
        super().__init__(*args, **kwargs)

    def list_crawlers(self) -> List:
        """:return: Lists of Crawlers"""
        conn = self.get_conn()
        return conn.get_crawlers()

    def get_iam_execution_role(self) -> Dict:
        """:return: iam role for crawler execution"""
        iam_client = self.get_client_type('iam', self.region_name)

        try:
            glue_execution_role = iam_client.get_role(RoleName=self.iam_role_name)
            self.log.info("Iam Role Name: %s", self.iam_role_name)
            return glue_execution_role
        except Exception as general_error:
            self.log.error("Failed to create aws glue crawler, error: %s", general_error)
            raise

    def initialize_crawler(self):
        """
        Initializes connection with AWS Glue to run crawler
        :return:
        """
        glue_client = self.get_conn()

        try:
            crawler_name = self.get_or_create_glue_crawler()
            crawler_run = glue_client.start_crawler(Name=crawler_name)
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
        glue_client = self.get_conn()
        crawler_run = glue_client.get_crawler(Name=crawler_name)
        crawler_run_state = crawler_run['Crawler']['State']
        return crawler_run_state

    def get_crawler_status(self, crawler_name: str) -> str:
        """
        Get current status of the Glue crawler. The crawler
        status can be succeeded, cancelled, or failed.
        :param crawler_name: unique crawler name per AWS account
        :type crawler_name: str
        :return: Status of the Glue crawler
        """
        glue_client = self.get_conn()
        crawler_run = glue_client.get_crawler(Name=crawler_name)
        crawler_run_status = crawler_run['Crawler']['LastCrawl']['Status']
        return crawler_run_status

    def crawler_completion(self, crawler_name: str) -> str:
        """
        Waits until Glue crawler with crawler_name completes or
        fails and returns final state if finished.
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
                crawler_run_status = self.get_crawler_status(crawler_name)
                if crawler_run_status in failed_status:
                    crawler_error_message = (
                        "Exiting Crawler: " + crawler_name + " Run State: " + crawler_run_state
                    )
                    self.log.info(crawler_error_message)
                    raise AirflowException(crawler_error_message)
                else:
                    self.log.info("Crawler Status: %s", crawler_run_status)
                    metrics = self.get_crawler_metrics(self.crawler_name)
                    print('Last Runtime Duration (seconds): ', metrics['LastRuntimeSeconds'])
                    print('Median Runtime Duration (seconds): ', metrics['MedianRuntimeSeconds'])
                    print('Tables Created: ', metrics['TablesCreated'])
                    print('Tables Updated: ', metrics['TablesUpdated'])
                    print('Tables Deleted: ', metrics['TablesDeleted'])

                    return {'Status': crawler_run_status}

            else:
                self.log.info(
                    "Polling for AWS Glue crawler: %s Current run state: %s",
                    self.crawler_name,
                    crawler_run_state,
                )
                time.sleep(self.CRAWLER_POLL_INTERVAL)

                metrics = self.get_crawler_metrics(self.crawler_name)
                time_left = int(metrics['TimeLeftSeconds'])

                if time_left > 0:
                    print('Estimated Time Left (seconds): ', time_left)
                    self.CRAWLER_POLL_INTERVAL = time_left
                else:
                    print('Crawler should finish soon')

    def get_or_create_glue_crawler(self) -> str:
        """
        Creates the crawler if the crawler doesn't exists and returns the crawler name
        :return:Name of the crawler
        """
        glue_client = self.get_conn()
        execution_role = self.get_iam_execution_role()
        crawler_config = {
            'Name': self.crawler_name,
            'Role': execution_role['Role']['RoleName'],
            'DatabaseName': self.db_name,
            'Description': self.crawler_desc,
            'Targets': self.targets,
            'Classifiers': self.classifiers,
            'TablePrefix': self.table_prefix,
            'SchemaChangePolicy': self.schema_change_policy,
            'RecrawlPolicy': self.recrawl_policy,
            'LineageConfiguration': {'CrawlerLineageSettings': self.lineage_settings},
            'Configuration': self.configuration,
            'CrawlerSecurityConfiguration': self.security_configuration,
            'Tags': self.tags,
        }
        try:
            get_crawler_response = glue_client.get_crawler(Name=self.crawler_name)
            self.log.info("Crawler already exists: %s", get_crawler_response['Crawler']['Name'])
            try:
                glue_client.update_crawler(**crawler_config)
                return get_crawler_response['Crawler']['Name']
            except Exception as general_error:
                self.log.error("Failed to update aws glue crawler, error: %s", general_error)
                raise
        except glue_client.exceptions.EntityNotFoundException:
            self.log.info("Crawler doesn't exist. Creating AWS Glue crawler")
            try:
                glue_client.create_crawler(**crawler_config)
                return self.crawler_name
            except Exception as general_error:
                self.log.error("Failed to create aws glue crawler, error: %s", general_error)
                raise

    def get_crawler_metrics(self, crawler_name):
        """
        Prints crawl runtime and glue catalog table metrics associated with the crawler
        :return:Dictionary of all the crawler metrics
        """
        glue_client = self.get_conn()
        crawler = glue_client.get_crawler_metrics(CrawlerNameList=[crawler_name])

        metrics = crawler['CrawlerMetricsList'][0]

        return metrics
