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
"""Sentry Integration."""

from __future__ import annotations

import logging
from functools import wraps
from typing import TYPE_CHECKING

from airflow.configuration import conf
from airflow.executors.executor_loader import ExecutorLoader
from airflow.utils.session import find_session_idx, provide_session
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.taskinstance import TaskInstance

log = logging.getLogger(__name__)


class DummySentry:
    """Blank class for Sentry."""

    def add_tagging(self, task_instance):
        """Blank function for tagging."""

    def add_breadcrumbs(self, task_instance, session: Session | None = None):
        """Blank function for breadcrumbs."""

    def enrich_errors(self, run):
        """Blank function for formatting a TaskInstance._run_raw_task."""
        return run

    def flush(self):
        """Blank function for flushing errors."""


Sentry: DummySentry = DummySentry()
if conf.getboolean("sentry", "sentry_on", fallback=False):
    import sentry_sdk
    from sentry_sdk.integrations.flask import FlaskIntegration
    from sentry_sdk.integrations.logging import ignore_logger

    class ConfiguredSentry(DummySentry):
        """Configure Sentry SDK."""

        SCOPE_DAG_RUN_TAGS = frozenset(("data_interval_end", "data_interval_start", "execution_date"))
        SCOPE_TASK_INSTANCE_TAGS = frozenset(("task_id", "dag_id", "try_number"))
        SCOPE_CRUMBS = frozenset(("task_id", "state", "operator", "duration"))

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
            ignore_logger("airflow.task")

            sentry_flask = FlaskIntegration()

            # LoggingIntegration is set by default.
            integrations = [sentry_flask]

            executor_class, _ = ExecutorLoader.import_default_executor_cls(validate=False)

            if executor_class.supports_sentry:
                from sentry_sdk.integrations.celery import CeleryIntegration

                sentry_celery = CeleryIntegration()
                integrations.append(sentry_celery)

            dsn = None
            sentry_config_opts = conf.getsection("sentry") or {}
            if sentry_config_opts:
                sentry_config_opts.pop("sentry_on")
                old_way_dsn = sentry_config_opts.pop("sentry_dsn", None)
                new_way_dsn = sentry_config_opts.pop("dsn", None)
                # supported backward compatibility with old way dsn option
                dsn = old_way_dsn or new_way_dsn

                unsupported_options = self.UNSUPPORTED_SENTRY_OPTIONS.intersection(sentry_config_opts.keys())
                if unsupported_options:
                    log.warning(
                        "There are unsupported options in [sentry] section: %s",
                        ", ".join(unsupported_options),
                    )

                sentry_config_opts["before_send"] = conf.getimport("sentry", "before_send", fallback=None)
                sentry_config_opts["transport"] = conf.getimport("sentry", "transport", fallback=None)

            if dsn:
                sentry_sdk.init(dsn=dsn, integrations=integrations, **sentry_config_opts)
            else:
                # Setting up Sentry using environment variables.
                log.debug("Defaulting to SENTRY_DSN in environment.")
                sentry_sdk.init(integrations=integrations, **sentry_config_opts)

        def add_tagging(self, task_instance):
            """Add tagging for a task_instance."""
            dag_run = task_instance.dag_run
            task = task_instance.task

            with sentry_sdk.configure_scope() as scope:
                for tag_name in self.SCOPE_TASK_INSTANCE_TAGS:
                    attribute = getattr(task_instance, tag_name)
                    scope.set_tag(tag_name, attribute)
                for tag_name in self.SCOPE_DAG_RUN_TAGS:
                    attribute = getattr(dag_run, tag_name)
                    scope.set_tag(tag_name, attribute)
                scope.set_tag("operator", task.__class__.__name__)

        @provide_session
        def add_breadcrumbs(
            self,
            task_instance: TaskInstance,
            session: Session | None = None,
        ) -> None:
            """Add breadcrumbs inside of a task_instance."""
            if session is None:
                return
            dr = task_instance.get_dagrun(session)
            task_instances = dr.get_task_instances(
                state={TaskInstanceState.SUCCESS, TaskInstanceState.FAILED},
                session=session,
            )

            for ti in task_instances:
                data = {}
                for crumb_tag in self.SCOPE_CRUMBS:
                    data[crumb_tag] = getattr(ti, crumb_tag)

                sentry_sdk.add_breadcrumb(category="completed_tasks", data=data, level="info")

        def enrich_errors(self, func):
            """
            Decorate errors.

            Wrap TaskInstance._run_raw_task to support task specific tags and breadcrumbs.
            """
            session_args_idx = find_session_idx(func)

            @wraps(func)
            def wrapper(_self, *args, **kwargs):
                # Wrapping the _run_raw_task function with push_scope to contain
                # tags and breadcrumbs to a specific Task Instance

                try:
                    session = kwargs.get("session", args[session_args_idx])
                except IndexError:
                    session = None

                with sentry_sdk.push_scope():
                    try:
                        # Is a LocalTaskJob get the task instance
                        if hasattr(_self, "task_instance"):
                            task_instance = _self.task_instance
                        else:
                            task_instance = _self

                        self.add_tagging(task_instance)
                        self.add_breadcrumbs(task_instance, session=session)
                        return func(_self, *args, **kwargs)
                    except Exception as e:
                        sentry_sdk.capture_exception(e)
                        raise

            return wrapper

        def flush(self):
            sentry_sdk.flush()

    Sentry = ConfiguredSentry()
