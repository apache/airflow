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
"""Default celery configuration."""

from __future__ import annotations

import json
import logging
import re
import ssl
from typing import Any

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.providers.celery.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.providers.common.compat.sdk import AirflowException

log = logging.getLogger(__name__)


def _broker_supports_visibility_timeout(url):
    return url.startswith(("redis://", "rediss://", "sqs://", "sentinel://"))


def get_default_celery_config(team_conf) -> dict[str, Any]:
    """
    Build Celery configuration using team-aware config.

    For Airflow versions < 3.2 that don't support multi-team configuration,
    falls back to using the global conf object.

    :param team_conf: ExecutorConf instance with team-specific configuration, or conf object
    :return: Dictionary with Celery configuration
    """
    # Check if team_conf supports team-specific config (has getsection method), if not then we know that
    # we're not running in a multi-team capable Airflow version, so we fallback to using the global conf.
    if not hasattr(team_conf, "getsection"):
        team_conf = conf

    broker_url = team_conf.get("celery", "BROKER_URL", fallback="redis://redis:6379/0")

    broker_transport_options: dict = team_conf.getsection("celery_broker_transport_options") or {}
    if "visibility_timeout" not in broker_transport_options:
        if _broker_supports_visibility_timeout(broker_url):
            broker_transport_options["visibility_timeout"] = 86400

    if "sentinel_kwargs" in broker_transport_options:
        try:
            sentinel_kwargs = json.loads(broker_transport_options["sentinel_kwargs"])
            if not isinstance(sentinel_kwargs, dict):
                raise ValueError
            broker_transport_options["sentinel_kwargs"] = sentinel_kwargs
        except Exception:
            raise AirflowException("sentinel_kwargs should be written in the correct dictionary format.")

    if team_conf.has_option("celery", "RESULT_BACKEND"):
        result_backend = team_conf.get_mandatory_value("celery", "RESULT_BACKEND")
    else:
        log.debug("Value for celery result_backend not found. Using sql_alchemy_conn with db+ prefix.")
        sql_alchemy_conn = team_conf.get("database", "SQL_ALCHEMY_CONN")
        # In SQLAlchemy 2.1 the default PostgreSQL driver changed from psycopg2 to psycopg (v3).
        # To maintain existing behavior, we explicitly specify psycopg2 for driverless PostgreSQL URLs.
        sql_alchemy_conn = sql_alchemy_conn.replace("postgresql://", "postgresql+psycopg2://", 1)
        result_backend = f"db+{sql_alchemy_conn}"

    # Handle result backend transport options (for Redis Sentinel support)
    result_backend_transport_options: dict = (
        team_conf.getsection("celery_result_backend_transport_options") or {}
    )
    if "sentinel_kwargs" in result_backend_transport_options:
        try:
            result_sentinel_kwargs = json.loads(result_backend_transport_options["sentinel_kwargs"])
            if not isinstance(result_sentinel_kwargs, dict):
                raise ValueError
            result_backend_transport_options["sentinel_kwargs"] = result_sentinel_kwargs
        except Exception:
            raise AirflowException(
                "sentinel_kwargs in [celery_result_backend_transport_options] should be written "
                "in the correct dictionary format."
            )

    extra_celery_config = team_conf.getjson("celery", "extra_celery_config", fallback={})

    config = {
        "accept_content": ["json"],
        "event_serializer": "json",
        "worker_prefetch_multiplier": team_conf.getint("celery", "worker_prefetch_multiplier", fallback=1),
        "task_acks_late": team_conf.getboolean("celery", "task_acks_late", fallback=True),
        "task_default_queue": team_conf.get("operators", "DEFAULT_QUEUE"),
        "task_default_exchange": team_conf.get("operators", "DEFAULT_QUEUE"),
        "task_track_started": team_conf.getboolean("celery", "task_track_started", fallback=True),
        "broker_url": broker_url,
        "broker_transport_options": broker_transport_options,
        "broker_connection_retry_on_startup": team_conf.getboolean(
            "celery", "broker_connection_retry_on_startup", fallback=True
        ),
        "result_backend": result_backend,
        "result_backend_transport_options": result_backend_transport_options,
        "database_engine_options": team_conf.getjson(
            "celery", "result_backend_sqlalchemy_engine_options", fallback={}
        ),
        "worker_concurrency": team_conf.getint("celery", "WORKER_CONCURRENCY", fallback=16),
        "worker_enable_remote_control": team_conf.getboolean(
            "celery", "worker_enable_remote_control", fallback=True
        ),
        **(extra_celery_config if isinstance(extra_celery_config, dict) else {}),
    }

    # In order to not change anything pre Task Execution API, we leave this setting as it was (unset) in Airflow2
    if AIRFLOW_V_3_0_PLUS:
        config.setdefault("worker_redirect_stdouts", False)
        config.setdefault("worker_hijack_root_logger", False)

    # Handle SSL configuration
    try:
        celery_ssl_active = team_conf.getboolean("celery", "SSL_ACTIVE", fallback=False)
    except AirflowConfigException:
        log.warning("Celery Executor will run without SSL")
        celery_ssl_active = False

    try:
        if celery_ssl_active:
            if broker_url and "amqp://" in broker_url:
                broker_use_ssl = {
                    "keyfile": team_conf.get("celery", "SSL_KEY"),
                    "certfile": team_conf.get("celery", "SSL_CERT"),
                    "ca_certs": team_conf.get("celery", "SSL_CACERT"),
                    "cert_reqs": ssl.CERT_REQUIRED,
                }
            elif broker_url and re.search("rediss?://|sentinel://", broker_url):
                broker_use_ssl = {
                    "ssl_keyfile": team_conf.get("celery", "SSL_KEY"),
                    "ssl_certfile": team_conf.get("celery", "SSL_CERT"),
                    "ssl_ca_certs": team_conf.get("celery", "SSL_CACERT"),
                    "ssl_cert_reqs": ssl.CERT_REQUIRED,
                }
            else:
                raise AirflowException(
                    "The broker you configured does not support SSL_ACTIVE to be True. "
                    "Please use RabbitMQ or Redis if you would like to use SSL for broker."
                )

            config["broker_use_ssl"] = broker_use_ssl
    except AirflowConfigException:
        raise AirflowException(
            "AirflowConfigException: SSL_ACTIVE is True, please ensure SSL_KEY, SSL_CERT and SSL_CACERT are set"
        )
    except Exception as e:
        raise AirflowException(
            f"Exception: There was an unknown Celery SSL Error. Please ensure you want to use SSL and/or have "
            f"all necessary certs and key ({e})."
        )

    # Warning for not recommended backends
    match_not_recommended_backend = re.search("rediss?://|amqp://|rpc://", result_backend)
    if match_not_recommended_backend:
        log.warning(
            "You have configured a result_backend using the protocol `%s`,"
            " it is highly recommended to use an alternative result_backend (i.e. a database).",
            match_not_recommended_backend.group(0).strip("://"),
        )

    return config


# IMPORTANT NOTE! Celery Executor has initialization done dynamically and it performs initialization when
# it is imported, so we need fallbacks here in order to be able to import the class directly without
# having configuration initialized before. Do not remove those fallbacks!
#
# This is not strictly needed for production:
#
#   * for Airflow 2.6 and before the defaults will come from the core defaults
#   * for Airflow 2.7+ the defaults will be loaded via ProvidersManager
#
# But it helps in our tests to import the executor class and validate if the celery code can be imported
# in the current and older versions of Airflow.

# For backward compatibility, keep DEFAULT_CELERY_CONFIG at module level using global config
# Use conf directly since we don't need team-specific config for the module-level default
DEFAULT_CELERY_CONFIG = get_default_celery_config(conf)
