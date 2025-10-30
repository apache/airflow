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
"""
Configured Sentry integration.

This module must only be imported conditionally since the Sentry SDK is NOT a
required dependency of the Airflow Task SDK. You shouldn't import this module
anyway, but use the parent ``airflow.sdk.execution_time.sentry`` path instead,
where things in this module are re-exported.
"""

from __future__ import annotations

import functools
from typing import TYPE_CHECKING

import sentry_sdk
import sentry_sdk.integrations.logging
import structlog

from airflow.sdk.execution_time.sentry.noop import NoopSentry
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger as Logger

    from airflow.sdk import Context
    from airflow.sdk.execution_time.sentry.noop import Run, RunReturn
    from airflow.sdk.types import DagRunProtocol, RuntimeTaskInstanceProtocol

log = structlog.get_logger(logger_name=__name__)


class ConfiguredSentry(NoopSentry):
    """Configure Sentry SDK."""

    SCOPE_DAG_RUN_TAGS = frozenset(("data_interval_end", "data_interval_start", "logical_date"))
    SCOPE_TASK_INSTANCE_TAGS = frozenset(("task_id", "dag_id", "try_number"))

    UNSUPPORTED_SENTRY_OPTIONS = frozenset(
        (
            "integrations",
            "in_app_include",
            "in_app_exclude",
            "ignore_errors",
            "before_breadcrumb",
        )
    )

    def __init__(self):
        """Initialize the Sentry SDK."""
        from airflow.configuration import conf

        sentry_sdk.integrations.logging.ignore_logger("airflow.task")

        # LoggingIntegration is set by default.
        integrations = []

        # TODO: How can we get executor info in the runner to support this?
        # executor_class, _ = ExecutorLoader.import_default_executor_cls()
        # if executor_class.supports_sentry:
        #     from sentry_sdk.integrations.celery import CeleryIntegration

        #     sentry_celery = CeleryIntegration()
        #     integrations.append(sentry_celery)

        dsn = None
        sentry_config_opts = conf.getsection("sentry") or {}
        if sentry_config_opts:
            sentry_config_opts.pop("sentry_on")
            old_way_dsn = sentry_config_opts.pop("sentry_dsn", None)
            new_way_dsn = sentry_config_opts.pop("dsn", None)
            # supported backward compatibility with old way dsn option
            dsn = old_way_dsn or new_way_dsn

            if unsupported_options := self.UNSUPPORTED_SENTRY_OPTIONS.intersection(sentry_config_opts):
                log.warning(
                    "There are unsupported options in [sentry] section",
                    options=unsupported_options,
                )

            sentry_config_opts["before_send"] = conf.getimport("sentry", "before_send", fallback=None)
            sentry_config_opts["transport"] = conf.getimport("sentry", "transport", fallback=None)

        if dsn:
            sentry_sdk.init(dsn=dsn, integrations=integrations, **sentry_config_opts)
        else:
            # Setting up Sentry using environment variables.
            log.debug("Defaulting to SENTRY_DSN in environment.")
            sentry_sdk.init(integrations=integrations, **sentry_config_opts)

    def add_tagging(self, dag_run: DagRunProtocol, task_instance: RuntimeTaskInstanceProtocol) -> None:
        """Add tagging for a task_instance."""
        task = task_instance.task

        with sentry_sdk.configure_scope() as scope:
            for tag_name in self.SCOPE_TASK_INSTANCE_TAGS:
                attribute = getattr(task_instance, tag_name)
                scope.set_tag(tag_name, attribute)
            for tag_name in self.SCOPE_DAG_RUN_TAGS:
                attribute = getattr(dag_run, tag_name)
                scope.set_tag(tag_name, attribute)
            scope.set_tag("operator", task.__class__.__name__)

    def add_breadcrumbs(self, task_instance: RuntimeTaskInstanceProtocol) -> None:
        """Add breadcrumbs inside of a task_instance."""
        breadcrumbs = RuntimeTaskInstance.get_task_breadcrumbs(
            dag_id=task_instance.dag_id,
            run_id=task_instance.run_id,
        )
        for breadcrumb in breadcrumbs:
            sentry_sdk.add_breadcrumb(category="completed_tasks", data=breadcrumb, level="info")

    def enrich_errors(self, run: Run) -> Run:
        """
        Decorate errors.

        Wrap :func:`airflow.sdk.execution_time.task_runner.run` to support task
        specific tags and breadcrumbs.
        """

        @functools.wraps(run)
        def wrapped_run(ti: RuntimeTaskInstance, context: Context, log: Logger) -> RunReturn:
            with sentry_sdk.push_scope():
                try:
                    self.add_tagging(context["dag_run"], ti)
                    self.add_breadcrumbs(ti)
                    return run(ti, context, log)
                except Exception as e:
                    sentry_sdk.capture_exception(e)
                    raise

        return wrapped_run

    def flush(self):
        sentry_sdk.flush()
