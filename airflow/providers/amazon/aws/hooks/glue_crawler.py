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
from typing import Dict, List, Optional
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

class AwsGlueCrawlerHook(AwsBaseHook):
    """
    Interacts with AWS Glue Crawler
    :param crawler_name = Unique crawler name per AWS Account
    :type crawler_name = Optional[str]
    :param crawler_desc = Crawler description
    :type crawler_desc = Optional[str]
    :param glue_db_name = AWS glue catalog database ID
    :type glue_db_name = Optional[str]
    :param iam_role_name = AWS IAM role for glue crawler
    :type iam_role_name = Optional[str]
    :param s3_target_configuration = Configurations for crawling AWS S3 paths
    :type s3_target_configuration = Optional[list]
    :param jdbc_target_configuration = Configurations for crawling JDBC paths
    :type jdbc_target_configuration = Optional[list]
    :param mongo_target_configuration = Configurations for crawling AWS DocumentDB or MongoDB
    :type mongo_target_configuration = Optional[list]
    :param dynamo_target_configuration = Configurations for crawling AWS DynamoDB
    :type dynamo_target_configuration = Optional[list]
    :param glue_catalog_target_configuration = Configurations for crawling AWS Glue CatalogDB
    :type glue_catalog_target_configuration = Optional[list]
    :param cron_schedule = Cron expression used to define the crawler schedule (e.g. cron(11 18 * ? * *))
    :type cron_schedule = Optional[str]
    :param classifiers = List of user defined custom classifiers to be used by the crawler 
    :type classifiers = Optional[list]
    :param table_prefix = Prefix for catalog table to be created
    :type table_prefix = Optional[str]
    :param update_behavior = Behavior when the crawler identifies schema changes
    :type update_behavior = Optional[str]
    :param delete_behavior = Behavior when the crawler identifies deleted objects
    :type delete_behavior = Optional[str]
    :param recrawl_behavior = Behavior when the crawler needs to crawl again
    :type recrawl_behavior = Optional[str]
    :param lineage_settings = Enables or disables data lineage 
    :type lineage_settings = Optional[str]
    :param json_configuration = Versioned JSON configuration for the crawler
    :type json_configuration = Optional[str]
    :param security_configuration = Name of the security configuration structure to be used by the crawler.
    :type security_configuration = Optional[str]
    :param tags = Tags to attach to the crawler request
    :type tags = Optional[dict]
    """

    CRAWLER_POLL_INTERVAL = 6  # polls crawler status after every CRAWLER_POLL_INTERVAL seconds

    def __init__(
        self, 
        crawler_name: Optional[str] = None,
        crawler_desc: Optional[str] = None,
        glue_db_name: str = 'default_db',
        iam_role_name: Optional[str] = None,
        s3_targets_configuration: Optional[list] = None,
        jdbc_targets_configuration: Optional[list] = None,
        mongo_targets_configuration: Optional[list] = None,
        dynamo_targets_configuration: Optional[list] = None,
        glue_catalog_targets_configuration: Optional[list] = None,
        cron_schedule: Optional[str] = None,
        classifiers: Optional[list] = None,
        table_prefix: Optional[list] = None,
        update_behavior: Optional[str] = None,
        delete_behavior: Optional[str] = None,
        recrawl_behavior: Optional[str] = None,
        lineage_settings: Optional[str] = None,
        json_configuration: Optional[str] = None,
        security_configuration: Optional[str] = None,
        tags: Optional[dict] = None,
        *args,
        **kwargs
    ):

        self.crawler_name = crawler_name
        self.crawler_desc = crawler_desc
        self.glue_db_name = glue_db_name
        self.iam_role_name = iam_role_name
        self.s3_targets_configuration = s3_targets_configuration
        self.jdbc_targets_configuration = jdbc_targets_configuration
        self.mongo_targets_configuration = mongo_targets_configuration
        self.dynamo_targets_configuration = dynamo_targets_configuration
        self.glue_catalog_targets_configuration = glue_catalog_targets_configuration
        self.cron_schedule = cron_schedule
        self.classifiers = classifiers
        self.table_prefix = table_prefix
        self.update_behavior = update_behavior
        self.delete_behavior = delete_behavior
        self.recrawl_behavior = recrawl_behavior
        self.lineage_settings = lineage_settings
        self.json_configuration = json_configuration
        self.security_configuration = security_configuration
        self.tags = tags
        self.aws_conn_id = aws_conn_id
        kwargs['client_type'] = 'glue'
        super().__init__(*args, **kwargs)
    
    def list_crawlers(self) -> List:
        ":return: Lists of Crawlers"
        conn = self.get_conn()
        return conn.get_crawlers()

    def get_iam_execution_role(self) -> Dict:
        ":return: iam role for crawler execution"
        iam_client = self.get_client_type('iam', self.region_name)

        try:
            glue_execution_role = iam_client.get_role(RoleName=self.role_name)
            self.log.info("Iam Role Name: %s", self.role_name)
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
                self.log.info(f"Crawler: {crawler_name} State: {crawler_run_state}")
                crawler_run_status = self.get_crawler_status(crawler_name)
                if crawler_run_status in failed_status:
                    crawler_error_message = "Exiting Crawler: " + cralwer_name + " Run State: " + crawler_run_state
                    self.log.info(crawler_error_message)
                    raise AirflowException(crawler_error_message)
                else:
                    self.log.info(f"Crawler Status: {crawler_run_status}")
                    return {'Status': crawler_run_status}
                    
            else:
                self.log.info(
                    f"Polling for AWS Glue crawler: {crawler_name} Current run state: {crawler_run_state}"
                )
                time.sleep(self.CRAWLER_POLL_INTERVAL)
                
                metrics = self.get_crawler_metrics(self.crawler_name)
                if metrics['StillEstimating']:
                    print('Estimated Time Left: Still Estimating')
                else:
                    time_left = int(metrics['TimeLeftSeconds'])
                    if time_left > 0:
                        print('Estimated Time Left: ', time_left)
                        sleep_secs = time_left
                    else:
                        print('Crawler should finish soon')

    def get_or_create_glue_crawler(self) -> str:
        """
        Creates the crawler if the crawler doesn't exists and returns the crawler name
        :return:Name of the crawler
        """
        glue_client = self.get_conn()
        try:
            get_crawler_response = glue_client.get_crawler(Name=self.crawler_name)
            self.log.info(f"Crawler already exists: {get_crawler_response['crawler']['Name']}")
            return get_crawler_response['Crawler']['Name']

        except glue_client.exceptions.EntityNotFoundException:
            self.log.info("Crawler doesn't exist. Creating AWS Glue crawler")
            if self.s3_bucket is None:
                raise AirflowException('Could not initialize glue crawler, error: Specify Parameter `s3_bucket`')
            execution_role = self.get_iam_execution_role()
            try:
                create_crawler_response = glue_client.create_crawler(
                    Name=self.crawler_name,
                    Role=execution_role['Role']['RoleName'],
                    DatabaseName=self.glue_db_name,
                    Description=self.crawler_desc,
                    Targets={
                        'S3Targets': self.s3_targets_configuration,
                        'JdbcTargets': self.jdbc_targets_configuration,
                        'MongoDBTargets': self.mongo_targets_configuration,
                        'DynamoDBTargets': self.dynamo_targets_configuration,
                        'CatalogTargets': self.glue_catalog_targets_configuration
                    },
                    Schedule=self.cron_schedule,
                    Classifiers=self.classifiers,
                    TablePrefix=self.table_prefix,
                    SchemaChangePolicy={
                        'UpdateBehavior': self.update_behavior,
                        'DeleteBehavior': self.delete_behavior
                    },
                    RecrawlPolicy={
                        'RecrawlBehavior': self.recrawl_behavior
                    },
                    LineageConfiguration={
                        'CrawlerLineageSettings': self.lineage_settings
                    },
                    Configuration=self.json_configuration,
                    CrawlerSecurityConfiguration=self.security_configuration,
                    Tags=self.tags
                )
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
        crawler = glue_client.get_crawler(Name=crawler_name)

        metrics = crawler['CrawlerMetricsList'][0]

        print('Last Runtime Duration (seconds): ', metrics['LastRuntimeSeconds'])
        print('Median Runtime Duration (seconds): ', metrics['MedianRuntimeSeconds'])
        print('Tables Created: ', metrics['TablesCreated'])
        print('Tables Updated: ', metrics['TablesUpdated'])
        print('Tables Deleted: ', metrics['TablesDeleted'])

        return metrics