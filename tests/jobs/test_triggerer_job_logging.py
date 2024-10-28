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

import importlib
import logging
import warnings

import pytest

from airflow.config_templates import airflow_local_settings
from airflow.jobs import triggerer_job_runner
from airflow.logging_config import configure_logging
from airflow.providers.amazon.aws.log.s3_task_handler import S3TaskHandler
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import RedirectStdHandler
from airflow.utils.log.trigger_handler import (
    DropTriggerLogsFilter,
    TriggererHandlerWrapper,
)

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.log_handlers import non_pytest_handlers


def assert_handlers(logger, *classes):
    handlers = non_pytest_handlers(logger.handlers)
    assert [x.__class__ for x in handlers] == list(classes or [])
    return handlers


@pytest.fixture(autouse=True)
def reload_triggerer_job():
    importlib.reload(triggerer_job_runner)


def test_configure_trigger_log_handler_file():
    """
    root logger: RedirectStdHandler
    task: FTH
    result: wrap

    """
    # reset logging
    root_logger = logging.getLogger()
    configure_logging()

    # before config
    assert_handlers(root_logger, RedirectStdHandler)

    # default task logger
    task_logger = logging.getLogger("airflow.task")
    task_handlers = assert_handlers(task_logger, FileTaskHandler)

    # not yet configured to use wrapper
    assert triggerer_job_runner.HANDLER_SUPPORTS_TRIGGERER is False

    triggerer_job_runner.configure_trigger_log_handler()
    # after config
    assert triggerer_job_runner.HANDLER_SUPPORTS_TRIGGERER is True
    root_handlers = assert_handlers(
        root_logger, RedirectStdHandler, TriggererHandlerWrapper
    )
    assert root_handlers[1].base_handler == task_handlers[0]
    # other handlers have DropTriggerLogsFilter
    assert root_handlers[0].filters[1].__class__ == DropTriggerLogsFilter
    # no filters on wrapper handler
    assert root_handlers[1].filters == []
    # wrapper handler uses handler from airflow.task
    assert root_handlers[1].base_handler.__class__ == FileTaskHandler


def test_configure_trigger_log_handler_s3():
    """
    root logger: RedirectStdHandler
    task: S3TH
    result: wrap
    """
    with conf_vars(
        {
            ("logging", "remote_logging"): "True",
            ("logging", "remote_log_conn_id"): "some_aws",
            ("logging", "remote_base_log_folder"): "s3://some-folder",
        }
    ):
        importlib.reload(airflow_local_settings)
        configure_logging()

    # before config
    root_logger = logging.getLogger()
    assert_handlers(root_logger, RedirectStdHandler)
    # default task logger
    task_logger = logging.getLogger("airflow.task")
    task_handlers = assert_handlers(task_logger, S3TaskHandler)
    # not yet configured to use wrapper
    assert triggerer_job_runner.HANDLER_SUPPORTS_TRIGGERER is False

    triggerer_job_runner.configure_trigger_log_handler()
    # after config
    assert triggerer_job_runner.HANDLER_SUPPORTS_TRIGGERER is True
    handlers = assert_handlers(root_logger, RedirectStdHandler, TriggererHandlerWrapper)
    assert handlers[1].base_handler == task_handlers[0]
    # other handlers have DropTriggerLogsFilter
    assert handlers[0].filters[1].__class__ == DropTriggerLogsFilter
    # no filters on wrapper handler
    assert handlers[1].filters == []
    # wrapper handler uses handler from airflow.task
    assert handlers[1].base_handler.__class__ == S3TaskHandler


class OldFileTaskHandler(FileTaskHandler):
    """Handler that hasn't been updated to support triggerer"""

    def _read(self, ti, try_number, metadata=None):
        super()._read(self, ti, try_number, metadata)


non_file_task_handler = {
    "version": 1,
    "handlers": {"task": {"class": "logging.Handler"}},
    "loggers": {"airflow.task": {"handlers": ["task"]}},
}

old_file_task_handler = {
    "version": 1,
    "handlers": {
        "task": {
            "class": "tests.jobs.test_triggerer_job_logging.OldFileTaskHandler",
            "base_log_folder": "hello",
        }
    },
    "loggers": {"airflow.task": {"handlers": ["task"]}},
}

not_supported_message = [
    "Handler OldFileTaskHandler does not support individual trigger logging. "
    "Please check the release notes for your provider to see if a newer version "
    "supports individual trigger logging.",
    "Could not find log handler suitable for individual trigger logging.",
]
not_found_message = [
    "Could not find log handler suitable for individual trigger logging."
]


@pytest.mark.parametrize(
    "cfg, cls, msg",
    [
        ("old_file_task_handler", OldFileTaskHandler, not_supported_message),
        ("non_file_task_handler", logging.Handler, not_found_message),
    ],
)
def test_configure_trigger_log_handler_not_file_task_handler(
    cfg, cls, msg, clear_all_logger_handlers
):
    """
    No root handler configured.
    When non FileTaskHandler is configured, don't modify.
    When an incompatible subclass of FileTaskHandler is configured, don't modify.
    """

    with conf_vars(
        {
            (
                "logging",
                "logging_config_class",
            ): f"tests.jobs.test_triggerer_job_logging.{cfg}",
        }
    ):
        importlib.reload(airflow_local_settings)
        configure_logging()

    # no root handlers
    root_logger = logging.getLogger()
    assert_handlers(root_logger)

    # default task logger
    task_logger = logging.getLogger("airflow.task")
    assert_handlers(task_logger, cls)

    # not yet configured to use wrapper
    assert triggerer_job_runner.HANDLER_SUPPORTS_TRIGGERER is False

    with warnings.catch_warnings(record=True) as captured:
        triggerer_job_runner.configure_trigger_log_handler()

    assert [x.message.args[0] for x in captured] == msg

    # after config
    # doesn't use TriggererHandlerWrapper, no change in handler
    assert triggerer_job_runner.HANDLER_SUPPORTS_TRIGGERER is False

    # still no root handlers
    assert_handlers(root_logger)


fallback_task = {
    "version": 1,
    "handlers": {
        "task": {
            "class": "airflow.providers.amazon.aws.log.s3_task_handler.S3TaskHandler",
            "base_log_folder": "~/abc",
            "s3_log_folder": "s3://abc",
        },
    },
    "loggers": {"airflow.task": {"handlers": ["task"]}},
}


def test_configure_trigger_log_handler_fallback_task():
    """
    root: no handler
    task: FTH
    result: wrap
    """
    with conf_vars(
        {
            (
                "logging",
                "logging_config_class",
            ): "tests.jobs.test_triggerer_job_logging.fallback_task",
        }
    ):
        importlib.reload(airflow_local_settings)
        configure_logging()

    # check custom config used
    task_logger = logging.getLogger("airflow.task")
    assert_handlers(task_logger, S3TaskHandler)

    # before config
    root_logger = logging.getLogger()
    assert_handlers(root_logger)
    assert triggerer_job_runner.HANDLER_SUPPORTS_TRIGGERER is False

    triggerer_job_runner.configure_trigger_log_handler()

    # after config
    assert triggerer_job_runner.HANDLER_SUPPORTS_TRIGGERER is True

    handlers = assert_handlers(root_logger, TriggererHandlerWrapper)
    assert handlers[0].base_handler == task_logger.handlers[0]
    # no filters on wrapper handler
    assert handlers[0].filters == []


root_has_task_handler = {
    "version": 1,
    "handlers": {
        "task": {"class": "logging.Handler"},
        "trigger": {
            "class": "airflow.utils.log.file_task_handler.FileTaskHandler",
            "base_log_folder": "blah",
        },
    },
    "loggers": {
        "airflow.task": {"handlers": ["task"]},
        "": {"handlers": ["trigger"]},
    },
}


def test_configure_trigger_log_handler_root_has_task_handler():
    """
    root logger: single handler that supports triggerer
    result: wrap
    """
    with conf_vars(
        {
            (
                "logging",
                "logging_config_class",
            ): "tests.jobs.test_triggerer_job_logging.root_has_task_handler",
        }
    ):
        configure_logging()

    # check custom config used
    task_logger = logging.getLogger("airflow.task")
    assert_handlers(task_logger, logging.Handler)

    # before config
    root_logger = logging.getLogger()
    assert_handlers(root_logger, FileTaskHandler)
    assert triggerer_job_runner.HANDLER_SUPPORTS_TRIGGERER is False

    # configure
    triggerer_job_runner.configure_trigger_log_handler()

    # after config
    assert triggerer_job_runner.HANDLER_SUPPORTS_TRIGGERER is True
    handlers = assert_handlers(root_logger, TriggererHandlerWrapper)
    # no filters on wrapper handler
    assert handlers[0].filters == []
    # wrapper handler uses handler from airflow.task
    assert handlers[0].base_handler.__class__ == FileTaskHandler


root_not_file_task = {
    "version": 1,
    "handlers": {
        "task": {
            "class": "airflow.providers.amazon.aws.log.s3_task_handler.S3TaskHandler",
            "base_log_folder": "~/abc",
            "s3_log_folder": "s3://abc",
        },
        "trigger": {"class": "logging.Handler"},
    },
    "loggers": {
        "airflow.task": {"handlers": ["task"]},
        "": {"handlers": ["trigger"]},
    },
}


def test_configure_trigger_log_handler_root_not_file_task(clear_all_logger_handlers):
    """
    root: A handler that doesn't support trigger or inherit FileTaskHandler
    task: Supports triggerer
    Result:
        * wrap and use the task logger handler
        * other root handlers filter trigger logging
    """
    with conf_vars(
        {
            (
                "logging",
                "logging_config_class",
            ): "tests.jobs.test_triggerer_job_logging.root_not_file_task",
        }
    ):
        configure_logging()

    # check custom config used
    task_logger = logging.getLogger("airflow.task")
    assert_handlers(task_logger, S3TaskHandler)

    # before config
    root_logger = logging.getLogger()
    assert_handlers(root_logger, logging.Handler)
    assert triggerer_job_runner.HANDLER_SUPPORTS_TRIGGERER is False

    # configure
    with warnings.catch_warnings(record=True) as captured:
        triggerer_job_runner.configure_trigger_log_handler()
    assert captured == []

    # after config
    assert triggerer_job_runner.HANDLER_SUPPORTS_TRIGGERER is True
    handlers = assert_handlers(root_logger, logging.Handler, TriggererHandlerWrapper)
    # other handlers have DropTriggerLogsFilter
    assert handlers[0].filters[0].__class__ == DropTriggerLogsFilter
    # no filters on wrapper handler
    assert handlers[1].filters == []
    # wrapper handler uses handler from airflow.task
    assert handlers[1].base_handler.__class__ == S3TaskHandler


root_logger_old_file_task = {
    "version": 1,
    "handlers": {
        "task": {
            "class": "airflow.providers.amazon.aws.log.s3_task_handler.S3TaskHandler",
            "base_log_folder": "~/abc",
            "s3_log_folder": "s3://abc",
        },
        "trigger": {
            "class": "tests.jobs.test_triggerer_job_logging.OldFileTaskHandler",
            "base_log_folder": "abc",
        },
    },
    "loggers": {
        "airflow.task": {"handlers": ["task"]},
        "": {"handlers": ["trigger"]},
    },
}


def test_configure_trigger_log_handler_root_old_file_task():
    """
    Root logger handler: An older subclass of FileTaskHandler that doesn't support triggerer
    Task logger handler: Supports triggerer
    Result:
        * wrap and use the task logger handler
        * other root handlers filter trigger logging
    """

    with conf_vars(
        {
            (
                "logging",
                "logging_config_class",
            ): "tests.jobs.test_triggerer_job_logging.root_logger_old_file_task",
        }
    ):
        configure_logging()

    # check custom config used
    assert_handlers(logging.getLogger("airflow.task"), S3TaskHandler)

    # before config
    root_logger = logging.getLogger()
    assert_handlers(root_logger, OldFileTaskHandler)

    assert triggerer_job_runner.HANDLER_SUPPORTS_TRIGGERER is False

    with warnings.catch_warnings(record=True) as captured:
        triggerer_job_runner.configure_trigger_log_handler()

    # since a root logger is explicitly configured with an old FileTaskHandler which doesn't
    # work properly with individual trigger logging, warn
    # todo: we should probably just remove the handler in this case it's basically misconfiguration
    assert [x.message.args[0] for x in captured] == [
        "Handler OldFileTaskHandler does not support individual trigger logging. "
        "Please check the release notes for your provider to see if a newer version "
        "supports individual trigger logging.",
    ]

    # after config
    assert triggerer_job_runner.HANDLER_SUPPORTS_TRIGGERER is True
    handlers = assert_handlers(root_logger, OldFileTaskHandler, TriggererHandlerWrapper)
    # other handlers have DropTriggerLogsFilter
    assert handlers[0].filters[0].__class__ == DropTriggerLogsFilter
    # no filters on wrapper handler
    assert handlers[1].filters == []
    # wrapper handler uses handler from airflow.task
    assert handlers[1].base_handler.__class__ == S3TaskHandler
