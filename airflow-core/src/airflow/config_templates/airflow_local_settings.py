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
"""Airflow logging settings."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from airflow.configuration import conf
from airflow.exceptions import AirflowException

if TYPE_CHECKING:
    from airflow.logging_config import RemoteLogIO, RemoteLogStreamIO

LOG_LEVEL: str = conf.get_mandatory_value("logging", "LOGGING_LEVEL").upper()


# Flask appbuilder's info level log is very verbose,
# so it's set to 'WARN' by default.
FAB_LOG_LEVEL: str = conf.get_mandatory_value("logging", "FAB_LOGGING_LEVEL").upper()

LOG_FORMAT: str = conf.get_mandatory_value("logging", "LOG_FORMAT")
DAG_PROCESSOR_LOG_FORMAT: str = conf.get_mandatory_value("logging", "DAG_PROCESSOR_LOG_FORMAT")

LOG_FORMATTER_CLASS: str = conf.get_mandatory_value(
    "logging", "LOG_FORMATTER_CLASS", fallback="airflow.utils.log.timezone_aware.TimezoneAware"
)

DAG_PROCESSOR_LOG_TARGET: str = conf.get_mandatory_value("logging", "DAG_PROCESSOR_LOG_TARGET")

BASE_LOG_FOLDER: str = os.path.expanduser(conf.get_mandatory_value("logging", "BASE_LOG_FOLDER"))

# This isn't used anymore, but kept for compat of people who might have imported it
DEFAULT_LOGGING_CONFIG: dict[str, Any] = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "airflow": {
            "format": LOG_FORMAT,
            "class": LOG_FORMATTER_CLASS,
        },
        "source_processor": {
            "format": DAG_PROCESSOR_LOG_FORMAT,
            "class": LOG_FORMATTER_CLASS,
        },
    },
    "filters": {
        "mask_secrets_core": {
            "()": "airflow._shared.secrets_masker._secrets_masker",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            # "class": "airflow.utils.log.logging_mixin.RedirectStdHandler",
            "formatter": "airflow",
            "stream": "sys.stdout",
            "filters": ["mask_secrets_core"],
        },
        "task": {
            "class": "airflow.utils.log.file_task_handler.FileTaskHandler",
            "formatter": "airflow",
            "base_log_folder": BASE_LOG_FOLDER,
            "filters": ["mask_secrets_core"],
        },
    },
    "loggers": {
        "airflow.task": {
            "handlers": ["task"],
            "level": LOG_LEVEL,
            # Set to true here (and reset via set_context) so that if no file is configured we still get logs!
            "propagate": True,
            "filters": ["mask_secrets_core"],
        },
        "flask_appbuilder": {
            "handlers": ["console"],
            "level": FAB_LOG_LEVEL,
            "propagate": True,
        },
    },
    "root": {
        "handlers": ["console"],
        "level": LOG_LEVEL,
        "filters": ["mask_secrets_core"],
    },
}

EXTRA_LOGGER_NAMES: str | None = conf.get("logging", "EXTRA_LOGGER_NAMES", fallback=None)
if EXTRA_LOGGER_NAMES:
    new_loggers = {
        logger_name.strip(): {
            "handlers": ["console"],
            "level": LOG_LEVEL,
            "propagate": True,
        }
        for logger_name in EXTRA_LOGGER_NAMES.split(",")
    }
    DEFAULT_LOGGING_CONFIG["loggers"].update(new_loggers)

##################
# Remote logging #
##################

REMOTE_LOGGING: bool = conf.getboolean("logging", "remote_logging")
REMOTE_TASK_LOG: RemoteLogIO | RemoteLogStreamIO | None = None
DEFAULT_REMOTE_CONN_ID: str | None = None


def _default_conn_name_from(mod_path, hook_name):
    # Try to set the default conn name from a hook, but don't error if something goes wrong at runtime
    from importlib import import_module

    global DEFAULT_REMOTE_CONN_ID

    try:
        mod = import_module(mod_path)

        hook = getattr(mod, hook_name)

        DEFAULT_REMOTE_CONN_ID = getattr(hook, "default_conn_name")
    except Exception:
        # Lets error in tests though!
        if "PYTEST_CURRENT_TEST" in os.environ:
            raise
        return None


if REMOTE_LOGGING:
    ELASTICSEARCH_HOST: str | None = conf.get("elasticsearch", "HOST")
    OPENSEARCH_HOST: str | None = conf.get("opensearch", "HOST")
    # Storage bucket URL for remote logging
    # S3 buckets should start with "s3://"
    # Cloudwatch log groups should start with "cloudwatch://"
    # GCS buckets should start with "gs://"
    # WASB buckets should start with "wasb"
    # HDFS path should start with "hdfs://"
    # just to help Airflow select correct handler
    remote_base_log_folder: str = conf.get_mandatory_value("logging", "remote_base_log_folder")
    remote_task_handler_kwargs = conf.getjson("logging", "remote_task_handler_kwargs", fallback={})
    if not isinstance(remote_task_handler_kwargs, dict):
        raise ValueError(
            "logging/remote_task_handler_kwargs must be a JSON object (a python dict), we got "
            f"{type(remote_task_handler_kwargs)}"
        )
    delete_local_copy = conf.getboolean("logging", "delete_local_logs")

    if remote_base_log_folder.startswith("s3://"):
        from airflow.providers.amazon.aws.log.s3_task_handler import S3RemoteLogIO

        _default_conn_name_from("airflow.providers.amazon.aws.hooks.s3", "S3Hook")
        REMOTE_TASK_LOG = S3RemoteLogIO(
            **(
                {
                    "base_log_folder": BASE_LOG_FOLDER,
                    "remote_base": remote_base_log_folder,
                    "delete_local_copy": delete_local_copy,
                }
                | remote_task_handler_kwargs
            )
        )
        remote_task_handler_kwargs = {}

    elif remote_base_log_folder.startswith("cloudwatch://"):
        from airflow.providers.amazon.aws.log.cloudwatch_task_handler import CloudWatchRemoteLogIO

        _default_conn_name_from("airflow.providers.amazon.aws.hooks.logs", "AwsLogsHook")
        url_parts = urlsplit(remote_base_log_folder)
        REMOTE_TASK_LOG = CloudWatchRemoteLogIO(
            **(
                {
                    "base_log_folder": BASE_LOG_FOLDER,
                    "remote_base": remote_base_log_folder,
                    "delete_local_copy": delete_local_copy,
                    "log_group_arn": url_parts.netloc + url_parts.path,
                }
                | remote_task_handler_kwargs
            )
        )
        remote_task_handler_kwargs = {}
    elif remote_base_log_folder.startswith("gs://"):
        from airflow.providers.google.cloud.log.gcs_task_handler import GCSRemoteLogIO

        _default_conn_name_from("airflow.providers.google.cloud.hooks.gcs", "GCSHook")
        key_path = conf.get_mandatory_value("logging", "google_key_path", fallback=None)

        REMOTE_TASK_LOG = GCSRemoteLogIO(
            **(
                {
                    "base_log_folder": BASE_LOG_FOLDER,
                    "remote_base": remote_base_log_folder,
                    "delete_local_copy": delete_local_copy,
                    "gcp_key_path": key_path,
                }
                | remote_task_handler_kwargs
            )
        )
        remote_task_handler_kwargs = {}
    elif remote_base_log_folder.startswith("wasb"):
        from airflow.providers.microsoft.azure.log.wasb_task_handler import WasbRemoteLogIO

        _default_conn_name_from("airflow.providers.microsoft.azure.hooks.wasb", "WasbHook")
        wasb_log_container = conf.get_mandatory_value(
            "azure_remote_logging", "remote_wasb_log_container", fallback="airflow-logs"
        )

        REMOTE_TASK_LOG = WasbRemoteLogIO(
            **(
                {
                    "base_log_folder": BASE_LOG_FOLDER,
                    "remote_base": remote_base_log_folder,
                    "delete_local_copy": delete_local_copy,
                    "wasb_container": wasb_log_container,
                }
                | remote_task_handler_kwargs
            )
        )
        remote_task_handler_kwargs = {}
    elif remote_base_log_folder.startswith("stackdriver://"):
        key_path = conf.get_mandatory_value("logging", "GOOGLE_KEY_PATH", fallback=None)
        # stackdriver:///airflow-tasks => airflow-tasks
        log_name = urlsplit(remote_base_log_folder).path[1:]
        STACKDRIVER_REMOTE_HANDLERS = {
            "task": {
                "class": "airflow.providers.google.cloud.log.stackdriver_task_handler.StackdriverTaskHandler",
                "formatter": "airflow",
                "gcp_log_name": log_name,
                "gcp_key_path": key_path,
            }
        }

        DEFAULT_LOGGING_CONFIG["handlers"].update(STACKDRIVER_REMOTE_HANDLERS)
    elif remote_base_log_folder.startswith("oss://"):
        from airflow.providers.alibaba.cloud.log.oss_task_handler import OSSRemoteLogIO

        _default_conn_name_from("airflow.providers.alibaba.cloud.hooks.oss", "OSSHook")

        REMOTE_TASK_LOG = OSSRemoteLogIO(
            **(
                {
                    "base_log_folder": BASE_LOG_FOLDER,
                    "remote_base": remote_base_log_folder,
                    "delete_local_copy": delete_local_copy,
                }
                | remote_task_handler_kwargs
            )
        )
        remote_task_handler_kwargs = {}
    elif remote_base_log_folder.startswith("hdfs://"):
        from airflow.providers.apache.hdfs.log.hdfs_task_handler import HdfsRemoteLogIO

        _default_conn_name_from("airflow.providers.apache.hdfs.hooks.webhdfs", "WebHDFSHook")

        REMOTE_TASK_LOG = HdfsRemoteLogIO(
            **(
                {
                    "base_log_folder": BASE_LOG_FOLDER,
                    "remote_base": urlsplit(remote_base_log_folder).path,
                    "delete_local_copy": delete_local_copy,
                }
                | remote_task_handler_kwargs
            )
        )
        remote_task_handler_kwargs = {}
    elif ELASTICSEARCH_HOST:
        ELASTICSEARCH_END_OF_LOG_MARK: str = conf.get_mandatory_value("elasticsearch", "END_OF_LOG_MARK")
        ELASTICSEARCH_FRONTEND: str = conf.get_mandatory_value("elasticsearch", "frontend")
        ELASTICSEARCH_WRITE_STDOUT: bool = conf.getboolean("elasticsearch", "WRITE_STDOUT")
        ELASTICSEARCH_WRITE_TO_ES: bool = conf.getboolean("elasticsearch", "WRITE_TO_ES")
        ELASTICSEARCH_JSON_FORMAT: bool = conf.getboolean("elasticsearch", "JSON_FORMAT")
        ELASTICSEARCH_JSON_FIELDS: str = conf.get_mandatory_value("elasticsearch", "JSON_FIELDS")
        ELASTICSEARCH_TARGET_INDEX: str = conf.get_mandatory_value("elasticsearch", "TARGET_INDEX")
        ELASTICSEARCH_HOST_FIELD: str = conf.get_mandatory_value("elasticsearch", "HOST_FIELD")
        ELASTICSEARCH_OFFSET_FIELD: str = conf.get_mandatory_value("elasticsearch", "OFFSET_FIELD")

        ELASTIC_REMOTE_HANDLERS: dict[str, dict[str, str | bool | None]] = {
            "task": {
                "class": "airflow.providers.elasticsearch.log.es_task_handler.ElasticsearchTaskHandler",
                "formatter": "airflow",
                "base_log_folder": BASE_LOG_FOLDER,
                "end_of_log_mark": ELASTICSEARCH_END_OF_LOG_MARK,
                "host": ELASTICSEARCH_HOST,
                "frontend": ELASTICSEARCH_FRONTEND,
                "write_stdout": ELASTICSEARCH_WRITE_STDOUT,
                "write_to_es": ELASTICSEARCH_WRITE_TO_ES,
                "target_index": ELASTICSEARCH_TARGET_INDEX,
                "json_format": ELASTICSEARCH_JSON_FORMAT,
                "json_fields": ELASTICSEARCH_JSON_FIELDS,
                "host_field": ELASTICSEARCH_HOST_FIELD,
                "offset_field": ELASTICSEARCH_OFFSET_FIELD,
            },
        }

        DEFAULT_LOGGING_CONFIG["handlers"].update(ELASTIC_REMOTE_HANDLERS)
    elif OPENSEARCH_HOST:
        OPENSEARCH_END_OF_LOG_MARK: str = conf.get_mandatory_value("opensearch", "END_OF_LOG_MARK")
        OPENSEARCH_PORT: str = conf.get_mandatory_value("opensearch", "PORT")
        OPENSEARCH_USERNAME: str = conf.get_mandatory_value("opensearch", "USERNAME")
        OPENSEARCH_PASSWORD: str = conf.get_mandatory_value("opensearch", "PASSWORD")
        OPENSEARCH_WRITE_STDOUT: bool = conf.getboolean("opensearch", "WRITE_STDOUT")
        OPENSEARCH_JSON_FORMAT: bool = conf.getboolean("opensearch", "JSON_FORMAT")
        OPENSEARCH_JSON_FIELDS: str = conf.get_mandatory_value("opensearch", "JSON_FIELDS")
        OPENSEARCH_HOST_FIELD: str = conf.get_mandatory_value("opensearch", "HOST_FIELD")
        OPENSEARCH_OFFSET_FIELD: str = conf.get_mandatory_value("opensearch", "OFFSET_FIELD")

        OPENSEARCH_REMOTE_HANDLERS: dict[str, dict[str, str | bool | None]] = {
            "task": {
                "class": "airflow.providers.opensearch.log.os_task_handler.OpensearchTaskHandler",
                "formatter": "airflow",
                "base_log_folder": BASE_LOG_FOLDER,
                "end_of_log_mark": OPENSEARCH_END_OF_LOG_MARK,
                "host": OPENSEARCH_HOST,
                "port": OPENSEARCH_PORT,
                "username": OPENSEARCH_USERNAME,
                "password": OPENSEARCH_PASSWORD,
                "write_stdout": OPENSEARCH_WRITE_STDOUT,
                "json_format": OPENSEARCH_JSON_FORMAT,
                "json_fields": OPENSEARCH_JSON_FIELDS,
                "host_field": OPENSEARCH_HOST_FIELD,
                "offset_field": OPENSEARCH_OFFSET_FIELD,
            },
        }
        DEFAULT_LOGGING_CONFIG["handlers"].update(OPENSEARCH_REMOTE_HANDLERS)
    else:
        raise AirflowException(
            "Incorrect remote log configuration. Please check the configuration of option 'host' in "
            "section 'elasticsearch' if you are using Elasticsearch. In the other case, "
            "'remote_base_log_folder' option in the 'logging' section."
        )
    DEFAULT_LOGGING_CONFIG["handlers"]["task"].update(remote_task_handler_kwargs)
