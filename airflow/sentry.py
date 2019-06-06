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

"""Sentry Integration"""


from typing import Any
from functools import wraps
from sqlalchemy import or_

from airflow import configuration as conf
from airflow.utils.db import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State


log = LoggingMixin().log


class DummySentry:
    """
    Blank class for Sentry.
    """

    @classmethod
    def add_tagging(cls, task_instance):
        """
        Blank function for tagging.
        """

    @classmethod
    def add_breadcrumbs(cls, session=None):
        """
        Blank function for breadcrumbs.
        """

    @classmethod
    def format_run_task(cls, run):
        """
        Blank function for formatting a TaskInstance._run_raw_task.
        """
        return run


@provide_session
def get_task_instances(dag_id, task_ids, execution_date, session=None):
    """
    Retrieves task instance based on dag_id and execution_date.
    """
    from airflow.models.taskinstance import TaskInstance  # Avoid circular import

    if session is None or not task_ids:
        return []

    TI = TaskInstance
    return (
        session.query(TI)
        .filter(
            TI.dag_id == dag_id,
            TI.task_id.in_(task_ids),
            TI.execution_date == execution_date,
            or_(TI.state == State.SUCCESS, TI.state == State.FAILED),
        )
        .all()
    )


class ConfiguredSentry:
    """
    Configure Sentry SDK.
    """

    SCOPE_TAGS = frozenset(("task_id", "dag_id", "execution_date", "operator"))
    SCOPE_CRUMBS = frozenset(
        ("dag_id", "task_id", "execution_date", "state", "operator", "duration")
    )

    def __init__(self):
        """
        Initialize the Sentry SDK.
        """
        ignore_logger("airflow.task")
        ignore_logger("airflow.jobs.backfill_job.BackfillJob")
        executor_name = conf.get("core", "EXECUTOR")

        sentry_flask = FlaskIntegration()

        # LoggingIntegration is set by default.
        integrations = [sentry_flask]

        if executor_name == "CeleryExecutor":
            from sentry_sdk.integrations.celery import CeleryIntegration
            from sentry_sdk.integrations.tornado import TornadoIntegration

            sentry_celery = CeleryIntegration()
            sentry_tornado = TornadoIntegration()
            integrations += [sentry_celery, sentry_tornado]

        dsn = conf.get("sentry", "sentry_dsn")
        if dsn:
            init(dsn=dsn, integrations=integrations)
        else:
            # Setting up Sentry using environment variables.
            log.debug("Defaulting to SENTRY_DSN in environment.")
            init(integrations=integrations)

    def add_tagging(self, task_instance):
        """
        Function to add tagging for a task_instance.
        """
        task = task_instance.task

        with configure_scope() as scope:
            for tag_name in self.SCOPE_TAGS:
                attribute = getattr(task_instance, tag_name)
                if tag_name == "operator":
                    attribute = task.__class__.__name__
                scope.set_tag(tag_name, attribute)

    @provide_session
    def add_breadcrumbs(self, task_instance, session=None):
        """
        Function to add breadcrumbs inside of a task_instance.
        """
        if session is None:
            return
        dag_id = task_instance.dag_id
        execution_date = task_instance.execution_date
        task = task_instance.task
        dag = task.dag
        task_ids = dag.task_ids
        task_instances = get_task_instances(
            dag_id, task_ids, execution_date, session=session
        )

        for ti in task_instances:
            data = {}
            for crumb_tag in self.SCOPE_CRUMBS:
                data[crumb_tag] = getattr(ti, crumb_tag)

            add_breadcrumb(category="completed_tasks", data=data, level="info")

    def format_run_task(self, func):
        """
        Function for formatting TaskInstance._run_raw_task.
        """

        @wraps(func)
        def wrapper(task_instance, *args, session=None, **kwargs):
            # Wrapping the _run_raw_task function with push_scope to contain
            # tags and breadcrumbs to a specific Task Instance
            with push_scope():
                self.add_tagging(task_instance)
                self.add_breadcrumbs(task_instance, session=session)
                try:
                    func(task_instance, *args, session=session, **kwargs)
                except Exception as e:
                    capture_exception(e)
                    raise e

        return wrapper


Sentry = DummySentry  # type: Any

try:
    from sentry_sdk.integrations.logging import ignore_logger
    from sentry_sdk.integrations.flask import FlaskIntegration
    from sentry_sdk import (
        push_scope,
        configure_scope,
        add_breadcrumb,
        init,
        capture_exception,
    )

    Sentry = ConfiguredSentry()

except ImportError as e:
    log.debug("Could not configure Sentry: %s, using DummySentry instead.", e)
