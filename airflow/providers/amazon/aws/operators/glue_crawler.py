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
from airflow.utils.decorators import apply_defaults


class AwsGlueCrawlerOperator(BaseOperator):
    """
    Creates, updates and triggers an AWS Glue Crawler. AWS Glue Crawler is a serverless
    extraction service that manages a catalog of metadata tables that contain the inferred 
    schema, format and data types of data stores within the AWS cloud.
    Language support: Python
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
        **kwargs,
    ):
        super().__init__(**kwargs)
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
        self.aws_conn_id = (aws_conn_id,)

    def execute(self, context):
        """
        Executes AWS Glue Crawler from Airflow
        :return: the name of the current glue crawler.
        """
        glue_crawler = AwsGlueCrawlerHook(
            crawler_name=self.crawler_name,
            crawler_desc=self.crawler_desc,
            db_name=self.db_name,
            iam_role_name=self.iam_role_name,
            targets=self.targets,
            classifiers=self.classifiers,
            table_prefix=self.table_prefix,
            schema_change_policy=self.schema_change_policy,
            recrawl_policy=self.recrawl_policy,
            lineage_settings=self.lineage_settings,
            configuration=self.configuration,
            security_configuration=self.security_configuration,
            tags=self.tags,
            aws_conn_id=self.aws_conn_id,
        )

        self.log.info("Initializing AWS Glue Crawler: %s", self.crawler_name)
        glue_crawler_run = glue_crawler.initialize_crawler()
        glue_crawler_run = glue_crawler.crawler_completion(self.crawler_name)
        self.log.info("AWS Glue Crawler: %s %s", self.crawler_name, glue_crawler_run)

        return self.crawler_name
