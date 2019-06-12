# -*- coding: utf-8 -*-
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

from sentry_sdk import init
from sentry_sdk.integrations.logging import ignore_logger

from airflow import configuration
from airflow.hooks.base_hook import BaseHook


class SentryHook(BaseHook):
    """
    Interact with Sentry. This class is a thin wrapper around the Sentry SDK.

    .. seealso:: the latest documentation `here <https://docs.sentry.io>`_.

    :param sentry_conn_id: The connection id to authenticate and get an object for Sentry
    :type sentry_conn_id: str
    """

    def __init__(self, sentry_conn_id="sentry_default"):
        super(SentryHook, self).__init__("sentry")
        self.sentry_conn_id = sentry_conn_id

    def get_conn(self):
        """
        Opens a connection with the Sentry SDK.

        :return: an authorized Sentry SDK session object
        :rtype: sentry_sdk.Client
        """
        integrations = []
        ignore_logger("airflow.task")
        ignore_logger("airflow.jobs.backfill_job.BackfillJob")
        executor_name = configuration.conf.get("core", "EXECUTOR")

        if executor_name == "CeleryExecutor":
            from sentry_sdk.integrations.celery import CeleryIntegration

            sentry_celery = CeleryIntegration()
            integrations = [sentry_celery]
        else:
            import logging
            from sentry_sdk.integrations.logging import LoggingIntegration

            sentry_logging = LoggingIntegration(
                level=logging.INFO, event_level=logging.ERROR
            )
            integrations = [sentry_logging]

        conn = self.get_connection(self.sentry_conn_id)

        return init(dsn=conn.host, integrations=integrations)
