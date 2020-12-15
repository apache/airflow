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

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.glue_crawler import AwsGlueCrawlerHook
from typing import Optional
from airflow.utils.decorators import apply_defaults

class AwsGlueCrawlerOperator(BaseOperator):
    """
    Creates an AWS Glue Crawler. AWS Glue Crawler is a serverless
    service for infering the schema, format and data type of data store on the AWS cloud.
    Language support: Python
    :param crawler_name = Unique crawler name per AWS Account
    :type crawler_name = Optional[str]
    :param crawler_desc = Crawler description
    :type crawler_desc = Optional[str]
    :param glue_db_name = AWS glue catalog database ID
    :type glue_db_name = Optional[str]
    :param iam_role_name = AWS IAM role for glue crawler
    :type iam_role_name = Optional[str]
    :param region_name = AWS region name (e.g. 'us-west-2')
    :type region_name = Optional[str]
    :param s3_targets_configuration = Configurations for crawling AWS S3 paths
    :type s3_targets_configuration = Optional[list]
    :param jdbc_targets_configuration = Configurations for crawling JDBC paths
    :type jdbc_targets_configuration = Optional[list]
    :param mongo_targets_configuration = Configurations for crawling AWS DocumentDB or MongoDB
    :type mongo_targets_configuration = Optional[list]
    :param dynamo_targets_configuration = Configurations for crawling AWS DynamoDB
    :type dynamo_targets_configuration = Optional[list]
    :param glue_catalog_targets_configuration = Configurations for crawling AWS Glue CatalogDB
    :type glue_catalog_targets_configuration = Optional[list]
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

    template_fields = ()
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
        self,
        *,
        crawler_name='aws_glue_default_crawler',
        crawler_desc='AWS Glue Crawler with Airflow',
        aws_conn_id='aws_default',
        glue_db_name=None,
        iam_role_name=None,
        region_name=None,
        s3_targets_configuration=None,
        jdbc_targets_configuration=None,
        mongo_targets_configuration=None,
        dynamo_targets_configuration=None,
        glue_catalog_targets_configuration=None,
        cron_schedule=None,
        classifiers=None,
        table_prefix=None,
        update_behavior=None,
        delete_behavior=None,
        recrawl_behavior=None,
        lineage_settings=None,
        json_configuration=None,
        security_configuration=None,
        tags=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.crawler_name = crawler_name
        self.crawler_desc = crawler_desc
        self.glue_db_name = glue_db_name
        self.iam_role_name = iam_role_name
        self.region_name = region_name
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

    def execute(self, context):
        """
        Executes AWS Glue Crawler from Airflow
        :return: the name of the current glue crawler.
        """

        glue_crawler = AwsGlueCrawlerHook(
            crawler_name=self.crawler_name,
            crawler_desc=self.crawler_desc,
            glue_db_name=self.glue_db_name,
            iam_role_name=self.iam_role_name,
            region_name=self.region_name,
            s3_targets_configuration=self.s3_targets_configuration,
            jdbc_targets_configuration=self.jdbc_targets_configuration,
            mongo_targets_configuration=self.mongo_targets_configuration,
            dynamo_targets_configuration=self.dynamo_targets_configuration,
            glue_catalog_targets_configuration=self.glue_catalog_targets_configuration,
            cron_schedule=self.cron_schedule,
            classifiers=self.classifiers,
            table_prefix=self.table_prefix,
            update_behavior=self.update_behavior,
            delete_behavior=self.delete_behavior,
            recrawl_behavior=self.recrawl_behavior,
            lineage_settings=self.lineage_settings,
            json_configuration=self.json_configuration,
            security_configuration=self.security_configuration,
            tags=self.tags,
            aws_conn_id=self.aws_conn_id
        )

        self.log.info("Initializing AWS Glue Crawler: %s", self.crawler_name)
        glue_crawler_run = glue_crawler.initialize_crawler()
        glue_crawler_run = glue_crawler.crawler_completion(self.crawler_name)
        self.log.info("AWS Glue Crawler: %s %s", self.crawler_name, glue_crawler_run)

        return self.crawler_name
