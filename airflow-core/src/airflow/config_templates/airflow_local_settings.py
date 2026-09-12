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

import inspect
import os
import warnings
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlsplit

from airflow.configuration import conf
from airflow.exceptions import AirflowException, RemovedInAirflow4Warning
from airflow.utils.log.file_task_handler import FileTaskHandler

if TYPE_CHECKING:
    from airflow.logging.remote import RemoteLogIO, RemoteLogStreamIO

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
# Default value for the ``[logging] logging_config_class`` option. Plain
# ``logging.config.dictConfig`` dict; the ``_class`` suffix on the config option
# is historical.
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

# Side-channel attributes read by ``discover_remote_log_handler`` from whichever
# module ``[logging] logging_config_class`` resolves through. Custom modules that
# override that option should define both at module scope to enable remote
# task-log read-back.
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


# First provider distribution version whose ``RemoteLogIO`` exposes ``from_config`` *and*
# registers the scheme in its provider.yaml ``remote-logging:`` block. Named in the deprecation
# message so a Deployment Manager knows exactly which upgrade retires the legacy branch.
_PROVIDER_DISPATCH_MIN_VERSIONS: dict[str, tuple[str, str]] = {
    "s3": ("apache-airflow-providers-amazon", "9.33.0"),
    "cloudwatch": ("apache-airflow-providers-amazon", "9.33.0"),
    "gs": ("apache-airflow-providers-google", "22.3.0"),
    "stackdriver": ("apache-airflow-providers-google", "22.3.0"),
    "wasb": ("apache-airflow-providers-microsoft-azure", "14.1.0"),
    "oss": ("apache-airflow-providers-alibaba", "3.4.0"),
    "hdfs": ("apache-airflow-providers-apache-hdfs", "4.13.0"),
    "elasticsearch": ("apache-airflow-providers-elasticsearch", "6.9.0"),
    "opensearch": ("apache-airflow-providers-opensearch", "1.12.0"),
}

# Scheme of ``[logging] remote_base_log_folder``; the key ProvidersManager dispatches on.
# Set below when remote logging is enabled.
_configured_scheme: str = ""


def _warn_legacy_remote_logging(remote_log_io: type, scheme: str) -> None:
    """
    Warn when this legacy branch, rather than provider dispatch, is what configures remote logging.

    ``airflow.logging_config._get_logging_config`` imports this module for its
    ``DEFAULT_LOGGING_CONFIG`` dict on every stock deployment, so the chain below still runs
    even when ProvidersManager scheme dispatch has already built the real handler. Warning
    unconditionally would therefore fire for every operator, including those with nothing left
    to migrate, so a branch warns only when dispatch cannot supersede it:

    * the installed provider predates ``from_config``, and so registers no scheme; or
    * ``[logging] remote_base_log_folder`` carries no scheme to dispatch on -- a bare
      ``wasb-logs`` path, or Elasticsearch/OpenSearch selected through their ``host`` option.
    """
    distribution, min_version = _PROVIDER_DISPATCH_MIN_VERSIONS[scheme]
    provider_supports_dispatch = hasattr(remote_log_io, "from_config")

    if provider_supports_dispatch and _configured_scheme == scheme:
        return

    if not provider_supports_dispatch:
        remedy = (
            f"Upgrade {distribution} to {min_version} or newer, which registers the {scheme!r} "
            f"scheme and builds this handler from {remote_log_io.__name__}.from_config()."
        )
    else:
        remedy = (
            f"{distribution} {min_version} or newer already registers the {scheme!r} scheme; set "
            f'[logging] remote_base_log_folder to a "{scheme}://" URL so it is dispatched on. '
            f"Keep the backend options you already set, as from_config() still reads them."
        )

    warnings.warn(
        f"Remote logging for {scheme!r} is being configured by the if/elif chain in "
        f"airflow_local_settings.py. That chain is deprecated and will be removed in Airflow 4, "
        f"after which remote logging is resolved only through provider registration. {remedy}",
        RemovedInAirflow4Warning,
        stacklevel=2,
    )


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
    _configured_scheme = urlsplit(remote_base_log_folder).scheme
    remote_task_handler_kwargs = conf.getjson("logging", "remote_task_handler_kwargs", fallback={})
    if not isinstance(remote_task_handler_kwargs, dict):
        raise ValueError(
            "logging/remote_task_handler_kwargs must be a JSON object (a python dict), we got "
            f"{type(remote_task_handler_kwargs)}"
        )
    _all_kwargs = cast("dict[str, Any]", remote_task_handler_kwargs)
    _fth_params = frozenset(inspect.signature(FileTaskHandler.__init__).parameters) - {
        "self",
        "base_log_folder",
    }
    _file_handler_kwargs = {k: v for k, v in _all_kwargs.items() if k in _fth_params}
    _io_kwargs = {k: v for k, v in _all_kwargs.items() if k not in _fth_params}
    delete_local_copy = conf.getboolean("logging", "delete_local_logs")

    if remote_base_log_folder.startswith("s3://"):
        from airflow.providers.amazon.aws.log.s3_task_handler import S3RemoteLogIO

        _warn_legacy_remote_logging(S3RemoteLogIO, "s3")
        _default_conn_name_from("airflow.providers.amazon.aws.hooks.s3", "S3Hook")
        REMOTE_TASK_LOG = S3RemoteLogIO(
            **cast(
                "dict[str, Any]",
                {
                    "base_log_folder": BASE_LOG_FOLDER,
                    "remote_base": remote_base_log_folder,
                    "delete_local_copy": delete_local_copy,
                }
                | _io_kwargs,
            )
        )

    elif remote_base_log_folder.startswith("cloudwatch://"):
        from airflow.providers.amazon.aws.log.cloudwatch_task_handler import CloudWatchRemoteLogIO

        _warn_legacy_remote_logging(CloudWatchRemoteLogIO, "cloudwatch")
        _default_conn_name_from("airflow.providers.amazon.aws.hooks.logs", "AwsLogsHook")
        url_parts = urlsplit(remote_base_log_folder)
        REMOTE_TASK_LOG = CloudWatchRemoteLogIO(
            **cast(
                "dict[str, Any]",
                {
                    "base_log_folder": BASE_LOG_FOLDER,
                    "remote_base": remote_base_log_folder,
                    "delete_local_copy": delete_local_copy,
                    "log_group_arn": url_parts.netloc + url_parts.path,
                }
                | _io_kwargs,
            )
        )

    elif remote_base_log_folder.startswith("gs://"):
        from airflow.providers.google.cloud.log.gcs_task_handler import GCSRemoteLogIO

        _warn_legacy_remote_logging(GCSRemoteLogIO, "gs")
        _default_conn_name_from("airflow.providers.google.cloud.hooks.gcs", "GCSHook")
        key_path = conf.get_mandatory_value("logging", "google_key_path", fallback=None)

        REMOTE_TASK_LOG = GCSRemoteLogIO(
            **cast(
                "dict[str, Any]",
                {
                    "base_log_folder": BASE_LOG_FOLDER,
                    "remote_base": remote_base_log_folder,
                    "delete_local_copy": delete_local_copy,
                    "gcp_key_path": key_path,
                }
                | _io_kwargs,
            )
        )

    elif remote_base_log_folder.startswith("wasb"):
        from airflow.providers.microsoft.azure.log.wasb_task_handler import WasbRemoteLogIO

        _warn_legacy_remote_logging(WasbRemoteLogIO, "wasb")
        _default_conn_name_from("airflow.providers.microsoft.azure.hooks.wasb", "WasbHook")
        wasb_log_container = conf.get_mandatory_value(
            "azure_remote_logging", "remote_wasb_log_container", fallback="airflow-logs"
        )

        # Handle both URI format (wasb://logs) and plain path (e.g., wasb-logs)
        wasb_remote_base = remote_base_log_folder.removeprefix("wasb://")

        REMOTE_TASK_LOG = WasbRemoteLogIO(
            **cast(
                "dict[str, Any]",
                {
                    "base_log_folder": BASE_LOG_FOLDER,
                    "remote_base": wasb_remote_base,
                    "delete_local_copy": delete_local_copy,
                    "wasb_container": wasb_log_container,
                }
                | _io_kwargs,
            )
        )

    elif remote_base_log_folder.startswith("stackdriver://"):
        from airflow.providers.google.cloud.log.stackdriver_task_handler import StackdriverRemoteLogIO

        _warn_legacy_remote_logging(StackdriverRemoteLogIO, "stackdriver")
        key_path = conf.get_mandatory_value("logging", "GOOGLE_KEY_PATH", fallback=None)
        # stackdriver:///airflow-tasks => airflow-tasks
        log_name = urlsplit(remote_base_log_folder).path[1:]

        REMOTE_TASK_LOG = StackdriverRemoteLogIO(
            **cast(
                "dict[str, Any]",
                {
                    "base_log_folder": BASE_LOG_FOLDER,
                    "gcp_log_name": log_name,
                    "gcp_key_path": key_path,
                    "delete_local_copy": delete_local_copy,
                }
                | _io_kwargs,
            )
        )

    elif remote_base_log_folder.startswith("oss://"):
        from airflow.providers.alibaba.cloud.log.oss_task_handler import OSSRemoteLogIO

        _warn_legacy_remote_logging(OSSRemoteLogIO, "oss")
        _default_conn_name_from("airflow.providers.alibaba.cloud.hooks.oss", "OSSHook")

        REMOTE_TASK_LOG = OSSRemoteLogIO(
            **cast(
                "dict[str, Any]",
                {
                    "base_log_folder": BASE_LOG_FOLDER,
                    "remote_base": remote_base_log_folder,
                    "delete_local_copy": delete_local_copy,
                }
                | _io_kwargs,
            )
        )

    elif remote_base_log_folder.startswith("hdfs://"):
        from airflow.providers.apache.hdfs.log.hdfs_task_handler import HdfsRemoteLogIO

        _warn_legacy_remote_logging(HdfsRemoteLogIO, "hdfs")
        _default_conn_name_from("airflow.providers.apache.hdfs.hooks.webhdfs", "WebHDFSHook")

        REMOTE_TASK_LOG = HdfsRemoteLogIO(
            **cast(
                "dict[str, Any]",
                {
                    "base_log_folder": BASE_LOG_FOLDER,
                    "remote_base": urlsplit(remote_base_log_folder).path,
                    "delete_local_copy": delete_local_copy,
                }
                | _io_kwargs,
            )
        )

    elif ELASTICSEARCH_HOST:
        from airflow.providers.elasticsearch.log.es_task_handler import ElasticsearchRemoteLogIO

        _warn_legacy_remote_logging(ElasticsearchRemoteLogIO, "elasticsearch")
        ELASTICSEARCH_WRITE_STDOUT: bool = conf.getboolean("elasticsearch", "WRITE_STDOUT")
        ELASTICSEARCH_WRITE_TO_ES: bool = conf.getboolean("elasticsearch", "WRITE_TO_ES")
        ELASTICSEARCH_JSON_FORMAT: bool = conf.getboolean("elasticsearch", "JSON_FORMAT")
        ELASTICSEARCH_TARGET_INDEX: str = conf.get_mandatory_value("elasticsearch", "TARGET_INDEX")
        ELASTICSEARCH_HOST_FIELD: str = conf.get_mandatory_value("elasticsearch", "HOST_FIELD")
        ELASTICSEARCH_OFFSET_FIELD: str = conf.get_mandatory_value("elasticsearch", "OFFSET_FIELD")
        ELASTICSEARCH_LOG_ID_TEMPLATE: str = conf.get_mandatory_value("elasticsearch", "LOG_ID_TEMPLATE")
        ELASTICSEARCH_END_OF_LOG_MARK: str = conf.get_mandatory_value("elasticsearch", "END_OF_LOG_MARK")
        ELASTICSEARCH_FRONTEND: str = conf.get_mandatory_value("elasticsearch", "FRONTEND")
        ELASTICSEARCH_JSON_FIELDS: str = conf.get_mandatory_value("elasticsearch", "JSON_FIELDS")

        ELASTICSEARCH_REMOTE_HANDLERS: dict[str, dict[str, str | bool | None]] = {
            "task": {
                "class": "airflow.providers.elasticsearch.log.es_task_handler.ElasticsearchTaskHandler",
                "formatter": "airflow",
                "base_log_folder": BASE_LOG_FOLDER,
                "end_of_log_mark": ELASTICSEARCH_END_OF_LOG_MARK,
                "host": ELASTICSEARCH_HOST,
                "frontend": ELASTICSEARCH_FRONTEND,
                "write_stdout": ELASTICSEARCH_WRITE_STDOUT,
                "write_to_es": ELASTICSEARCH_WRITE_TO_ES,
                "json_format": ELASTICSEARCH_JSON_FORMAT,
                "json_fields": ELASTICSEARCH_JSON_FIELDS,
                "host_field": ELASTICSEARCH_HOST_FIELD,
                "offset_field": ELASTICSEARCH_OFFSET_FIELD,
            },
        }
        DEFAULT_LOGGING_CONFIG["handlers"].update(ELASTICSEARCH_REMOTE_HANDLERS)

        REMOTE_TASK_LOG = ElasticsearchRemoteLogIO(
            host=ELASTICSEARCH_HOST,
            target_index=ELASTICSEARCH_TARGET_INDEX,
            write_stdout=ELASTICSEARCH_WRITE_STDOUT,
            write_to_es=ELASTICSEARCH_WRITE_TO_ES,
            offset_field=ELASTICSEARCH_OFFSET_FIELD,
            host_field=ELASTICSEARCH_HOST_FIELD,
            base_log_folder=BASE_LOG_FOLDER,
            delete_local_copy=delete_local_copy,
            json_format=ELASTICSEARCH_JSON_FORMAT,
            log_id_template=ELASTICSEARCH_LOG_ID_TEMPLATE,
        )

    elif OPENSEARCH_HOST:
        from airflow.providers.opensearch.log.os_task_handler import OpensearchRemoteLogIO

        _warn_legacy_remote_logging(OpensearchRemoteLogIO, "opensearch")
        # ``[opensearch] port`` declares an empty-string default, so the key is always present and
        # ``conf.getint`` raises on ``int("")`` instead of falling back to 9200.
        _opensearch_port = conf.get("opensearch", "PORT", fallback="")
        OPENSEARCH_PORT = int(_opensearch_port) if _opensearch_port else 9200
        OPENSEARCH_USERNAME: str = conf.get_mandatory_value("opensearch", "USERNAME")
        OPENSEARCH_PASSWORD: str = conf.get_mandatory_value("opensearch", "PASSWORD")
        OPENSEARCH_WRITE_STDOUT: bool = conf.getboolean("opensearch", "WRITE_STDOUT")
        OPENSEARCH_WRITE_TO_OS: bool = conf.getboolean("opensearch", "WRITE_TO_OS")
        OPENSEARCH_JSON_FORMAT: bool = conf.getboolean("opensearch", "JSON_FORMAT")
        OPENSEARCH_TARGET_INDEX: str = conf.get_mandatory_value("opensearch", "TARGET_INDEX")
        OPENSEARCH_HOST_FIELD: str = conf.get_mandatory_value("opensearch", "HOST_FIELD")
        OPENSEARCH_OFFSET_FIELD: str = conf.get_mandatory_value("opensearch", "OFFSET_FIELD")
        OPENSEARCH_LOG_ID_TEMPLATE: str = conf.get("opensearch", "LOG_ID_TEMPLATE", fallback="") or (
            "{dag_id}-{task_id}-{run_id}-{map_index}-{try_number}"
        )

        REMOTE_TASK_LOG = OpensearchRemoteLogIO(
            host=OPENSEARCH_HOST,
            port=OPENSEARCH_PORT,
            username=OPENSEARCH_USERNAME,
            password=OPENSEARCH_PASSWORD,
            target_index=OPENSEARCH_TARGET_INDEX,
            write_stdout=OPENSEARCH_WRITE_STDOUT,
            write_to_opensearch=OPENSEARCH_WRITE_TO_OS,
            offset_field=OPENSEARCH_OFFSET_FIELD,
            host_field=OPENSEARCH_HOST_FIELD,
            base_log_folder=BASE_LOG_FOLDER,
            delete_local_copy=delete_local_copy,
            json_format=OPENSEARCH_JSON_FORMAT,
            log_id_template=OPENSEARCH_LOG_ID_TEMPLATE,
        )
    else:
        raise AirflowException(
            "Incorrect remote log configuration. Please check the configuration of option 'host' in "
            "section 'elasticsearch' if you are using Elasticsearch. In the other case, "
            "'remote_base_log_folder' option in the 'logging' section."
        )
    DEFAULT_LOGGING_CONFIG["handlers"]["task"].update(_file_handler_kwargs)
