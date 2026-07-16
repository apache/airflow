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

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from botocore.exceptions import ClientError

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.triggers.glue_crawler import GlueCrawlerCompleteTrigger
from airflow.providers.amazon.aws.utils import validate_execute_complete_event
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.common.compat.sdk import AirflowException, conf

if TYPE_CHECKING:
    from airflow.sdk import Context


class _GlueCrawlerBaseOperator(AwsBaseOperator[GlueCrawlerHook]):
    aws_hook_class = GlueCrawlerHook
    ui_color = "#ededed"


class GlueCrawlerCreateOperator(_GlueCrawlerBaseOperator):
    """
    Create an AWS Glue crawler.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GlueCrawlerCreateOperator`

    :param config: Configurations for the AWS Glue crawler.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    template_fields: Sequence[str] = aws_template_fields("config")

    def __init__(self, *, config: dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.config = config

    def execute(self, context: Context) -> str:
        crawler_name = self.config["Name"]
        self.hook.create_crawler(**self.config)
        return crawler_name


class GlueCrawlerUpdateOperator(_GlueCrawlerBaseOperator):
    """
    Update an existing AWS Glue crawler.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GlueCrawlerUpdateOperator`

    :param config: Configurations for the AWS Glue crawler.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    template_fields: Sequence[str] = aws_template_fields("config")

    def __init__(self, *, config: dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.config = config

    def execute(self, context: Context) -> str:
        crawler_name = self.config["Name"]
        self.hook.update_crawler(**self.config)
        return crawler_name


class GlueCrawlerRunOperator(_GlueCrawlerBaseOperator):
    """
    Run an existing AWS Glue crawler.

    AWS Glue Crawler is a serverless service that manages a catalog of
    metadata tables that contain the inferred schema, format and data
    types of data stores within the AWS cloud.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GlueCrawlerRunOperator`

    :param crawler_name: Name of the AWS Glue crawler.
    :param poll_interval: Time (in seconds) to wait between two consecutive calls to check crawler status
    :param wait_for_completion: Whether to wait for crawl execution completion. (default: True)
    :param deferrable: If True, the operator will wait asynchronously for the crawl to complete.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
    :param fail_on_already_running: If True (default), the operator will raise an exception when
        ``start_crawler()`` encounters a ``CrawlerRunningException`` (i.e., the crawler is already
        running). If False, the operator continues with the existing run and waits for it if
        ``wait_for_completion`` is True. Setting this to False is useful for handling retry-induced race
        conditions where boto3 retries trigger a second ``start_crawler()`` call after a network timeout
        on the first successful call. (default: True)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    template_fields: Sequence[str] = aws_template_fields("crawler_name")

    def __init__(
        self,
        *,
        crawler_name: str,
        poll_interval: int = 5,
        wait_for_completion: bool = True,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        fail_on_already_running: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.poll_interval = poll_interval
        self.wait_for_completion = wait_for_completion
        self.deferrable = deferrable
        self.fail_on_already_running = fail_on_already_running
        self.crawler_name = crawler_name

    def execute(self, context: Context) -> str:
        """
        Run an AWS Glue crawler from Airflow.

        :return: The name of the current Glue crawler.
        """
        self.log.info("Triggering AWS Glue Crawler")
        try:
            self.hook.start_crawler(self.crawler_name)
        except ClientError as e:
            if not self.fail_on_already_running and e.response["Error"]["Code"] == "CrawlerRunningException":
                self.log.warning(
                    "Crawler '%s' is already running. Continuing with the existing run instead of failing.",
                    self.crawler_name,
                )
            else:
                raise

        if self.deferrable:
            self.defer(
                trigger=GlueCrawlerCompleteTrigger(
                    crawler_name=self.crawler_name,
                    waiter_delay=self.poll_interval,
                    aws_conn_id=self.aws_conn_id,
                    region_name=self.region_name,
                    verify=self.verify,
                    botocore_config=self.botocore_config,
                ),
                method_name="execute_complete",
            )
        elif self.wait_for_completion:
            self.log.info("Waiting for AWS Glue Crawler")
            self.hook.wait_for_crawler_completion(
                crawler_name=self.crawler_name, poll_interval=self.poll_interval
            )

        return self.crawler_name

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> str:
        validated_event = validate_execute_complete_event(event)

        if validated_event["status"] != "success":
            raise AirflowException(f"Error in glue crawl: {validated_event}")
        return self.crawler_name


class GlueCrawlerDeleteOperator(_GlueCrawlerBaseOperator):
    """
    Delete an AWS Glue crawler.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GlueCrawlerDeleteOperator`

    :param crawler_name: Name of the AWS Glue crawler.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    template_fields: Sequence[str] = aws_template_fields("crawler_name")

    def __init__(self, *, crawler_name: str, **kwargs):
        super().__init__(**kwargs)
        self.crawler_name = crawler_name

    def execute(self, context: Context) -> None:
        self.log.info("Deleting AWS Glue crawler %s", self.crawler_name)
        self.hook.glue_client.delete_crawler(Name=self.crawler_name)
        self.log.info("Deleted AWS Glue crawler %s", self.crawler_name)


# TODO: Remove GlueCrawlerOperator in Amazon provider 11.0.0 or later.
class GlueCrawlerOperator(GlueCrawlerRunOperator):
    """
    Create, update and run an AWS Glue crawler.

    .. deprecated::
        Use :class:`GlueCrawlerCreateOperator`, :class:`GlueCrawlerUpdateOperator`, and
        :class:`GlueCrawlerRunOperator` instead.

    :param config: Configurations for the AWS Glue crawler.
    :param poll_interval: Time (in seconds) to wait between two consecutive calls to check crawler status.
    :param wait_for_completion: Whether to wait for crawl execution completion. (default: True)
    :param deferrable: If True, the operator will wait asynchronously for the crawl to complete.
        This implies waiting for completion. (default: False)
    :param fail_on_already_running: If True (default), raise an exception if the crawler is already running.
        If False, wait for the existing run to complete.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    template_fields: Sequence[str] = aws_template_fields("config")

    def __init__(
        self,
        config: dict[str, Any],
        poll_interval: int = 5,
        wait_for_completion: bool = True,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        fail_on_already_running: bool = True,
        **kwargs,
    ):
        warnings.warn(
            "GlueCrawlerOperator is deprecated. Use GlueCrawlerCreateOperator, "
            "GlueCrawlerUpdateOperator, and GlueCrawlerRunOperator instead.",
            category=AirflowProviderDeprecationWarning,
            stacklevel=2,
        )
        super().__init__(
            crawler_name=config["Name"],
            poll_interval=poll_interval,
            wait_for_completion=wait_for_completion,
            deferrable=deferrable,
            fail_on_already_running=fail_on_already_running,
            **kwargs,
        )
        self.config = config

    def execute(self, context: Context) -> str:
        self.crawler_name = self.config["Name"]
        if self.hook.has_crawler(self.crawler_name):
            try:
                self.hook.update_crawler(**self.config)
            except ClientError as e:
                if (
                    not self.fail_on_already_running
                    and e.response["Error"]["Code"] == "CrawlerRunningException"
                ):
                    self.log.warning(
                        "Crawler '%s' is currently running. "
                        "Skipping update and waiting for the existing run to complete.",
                        self.crawler_name,
                    )
                else:
                    raise
        else:
            self.hook.create_crawler(**self.config)
        return super().execute(context)

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> str:
        super().execute_complete(context, event)
        return self.config["Name"]
