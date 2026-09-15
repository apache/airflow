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

import importlib
import inspect
import json
import warnings
from unittest import mock

import pytest

from airflow.config_templates import airflow_local_settings
from airflow.exceptions import RemovedInAirflow4Warning
from airflow.utils.log.file_task_handler import FileTaskHandler

from tests_common.test_utils.config import conf_vars

REMOTE_IO_PATHS = {
    "s3": "airflow.providers.amazon.aws.log.s3_task_handler.S3RemoteLogIO",
    "cloudwatch": "airflow.providers.amazon.aws.log.cloudwatch_task_handler.CloudWatchRemoteLogIO",
    "gs": "airflow.providers.google.cloud.log.gcs_task_handler.GCSRemoteLogIO",
    "stackdriver": "airflow.providers.google.cloud.log.stackdriver_task_handler.StackdriverRemoteLogIO",
    "wasb": "airflow.providers.microsoft.azure.log.wasb_task_handler.WasbRemoteLogIO",
    "oss": "airflow.providers.alibaba.cloud.log.oss_task_handler.OSSRemoteLogIO",
    "hdfs": "airflow.providers.apache.hdfs.log.hdfs_task_handler.HdfsRemoteLogIO",
    "elasticsearch": "airflow.providers.elasticsearch.log.es_task_handler.ElasticsearchRemoteLogIO",
    "opensearch": "airflow.providers.opensearch.log.os_task_handler.OpensearchRemoteLogIO",
}

REMOTE_IO_PROVIDERS = [
    ("s3://bucket/path", REMOTE_IO_PATHS["s3"]),
    ("wasb-logs", REMOTE_IO_PATHS["wasb"]),
    ("gs://bucket/path", REMOTE_IO_PATHS["gs"]),
    ("cloudwatch://arn:aws:logs:us-east-1:0:log-group:foo", REMOTE_IO_PATHS["cloudwatch"]),
    ("oss://bucket/path", REMOTE_IO_PATHS["oss"]),
    ("hdfs://host/path", REMOTE_IO_PATHS["hdfs"]),
    ("stackdriver://host/path", REMOTE_IO_PATHS["stackdriver"]),
]
REMOTE_IO_IDS = ["s3", "wasb", "gcs", "cloudwatch", "oss", "hdfs", "stackdriver"]


@pytest.fixture
def restore_local_settings():
    yield
    importlib.reload(airflow_local_settings)


@pytest.mark.parametrize(("remote_base", "remote_io_path"), REMOTE_IO_PROVIDERS, ids=REMOTE_IO_IDS)
def test_io_kwargs_forwarded_to_remote_log_io(remote_base, remote_io_path, restore_local_settings):
    """IO-level kwargs reach the RemoteLogIO constructor and don't leak into the handler config."""
    pytest.importorskip(remote_io_path.rsplit(".", 1)[0])
    io_kwargs = {"remote_base": "ignored", "custom_key": "v"}
    with (
        mock.patch(remote_io_path) as mock_remote_io,
        conf_vars(
            {
                ("logging", "remote_logging"): "True",
                ("logging", "remote_base_log_folder"): remote_base,
                ("logging", "remote_task_handler_kwargs"): json.dumps(io_kwargs),
            }
        ),
    ):
        importlib.reload(airflow_local_settings)
        task_cfg = airflow_local_settings.DEFAULT_LOGGING_CONFIG["handlers"]["task"]
        for k in io_kwargs:
            assert k not in task_cfg, f"IO kwarg {k!r} leaked into task handler config"

        for k, v in io_kwargs.items():
            assert mock_remote_io.call_args.kwargs[k] == v


@pytest.mark.parametrize(("remote_base", "remote_io_path"), REMOTE_IO_PROVIDERS, ids=REMOTE_IO_IDS)
def test_handler_kwargs_reach_file_task_handler(remote_base, remote_io_path, restore_local_settings):
    """Handler-level kwargs (max_bytes, backup_count, delay) reach the FileTaskHandler config."""
    pytest.importorskip(remote_io_path.rsplit(".", 1)[0])
    handler_kwargs = {"max_bytes": 5_000_000, "backup_count": 5}
    with (
        mock.patch(remote_io_path) as mock_remote_io,
        conf_vars(
            {
                ("logging", "remote_logging"): "True",
                ("logging", "remote_base_log_folder"): remote_base,
                ("logging", "remote_task_handler_kwargs"): json.dumps(handler_kwargs),
            }
        ),
    ):
        importlib.reload(airflow_local_settings)
        task_cfg = airflow_local_settings.DEFAULT_LOGGING_CONFIG["handlers"]["task"]
        for k, v in handler_kwargs.items():
            assert task_cfg[k] == v, f"Handler kwarg {k!r} not found in task handler config"

        for k in handler_kwargs:
            assert k not in mock_remote_io.call_args.kwargs, (
                f"Handler kwarg {k!r} leaked into RemoteLogIO constructor"
            )


@pytest.mark.parametrize(("remote_base", "remote_io_path"), REMOTE_IO_PROVIDERS, ids=REMOTE_IO_IDS)
def test_mixed_kwargs_split_correctly(remote_base, remote_io_path, restore_local_settings):
    """When both handler and IO kwargs are present, each goes to the right place."""
    pytest.importorskip(remote_io_path.rsplit(".", 1)[0])
    mixed_kwargs = {"max_bytes": 5_000_000, "backup_count": 5, "custom_io_key": "val"}
    with (
        mock.patch(remote_io_path) as mock_remote_io,
        conf_vars(
            {
                ("logging", "remote_logging"): "True",
                ("logging", "remote_base_log_folder"): remote_base,
                ("logging", "remote_task_handler_kwargs"): json.dumps(mixed_kwargs),
            }
        ),
    ):
        importlib.reload(airflow_local_settings)
        task_cfg = airflow_local_settings.DEFAULT_LOGGING_CONFIG["handlers"]["task"]

        assert task_cfg["max_bytes"] == 5_000_000
        assert task_cfg["backup_count"] == 5
        assert "custom_io_key" not in task_cfg

        assert mock_remote_io.call_args.kwargs["custom_io_key"] == "val"
        assert "max_bytes" not in mock_remote_io.call_args.kwargs
        assert "backup_count" not in mock_remote_io.call_args.kwargs


@pytest.mark.parametrize(
    ("configured_port", "expected_port"),
    [
        pytest.param("", 9200, id="unset-falls-back-to-9200"),
        pytest.param("9201", 9201, id="explicit-port-is-an-int"),
    ],
)
def test_opensearch_port_resolution(configured_port, expected_port, restore_local_settings):
    """``[opensearch] port`` defaults to an empty string, which must not blow up module import."""
    remote_io_path = "airflow.providers.opensearch.log.os_task_handler.OpensearchRemoteLogIO"
    pytest.importorskip(remote_io_path.rsplit(".", 1)[0])
    with (
        mock.patch(remote_io_path) as mock_remote_io,
        conf_vars(
            {
                ("logging", "remote_logging"): "True",
                ("logging", "remote_base_log_folder"): "",
                ("elasticsearch", "host"): "",
                ("opensearch", "host"): "https://opensearch.example.com:9202",
                ("opensearch", "port"): configured_port,
            }
        ),
    ):
        importlib.reload(airflow_local_settings)

        assert mock_remote_io.call_args.kwargs["port"] == expected_port


def test_file_handler_params_introspected_correctly():
    """The introspected FileTaskHandler params include the expected kwargs."""
    init_params = set(inspect.signature(FileTaskHandler.__init__).parameters) - {"self", "base_log_folder"}
    assert {"max_bytes", "backup_count", "delay"} <= init_params


DISPATCHABLE_BACKENDS = [
    pytest.param("s3://bucket/path", {}, REMOTE_IO_PATHS["s3"], id="s3"),
    pytest.param(
        "cloudwatch://arn:aws:logs:us-east-1:0:log-group:foo",
        {},
        REMOTE_IO_PATHS["cloudwatch"],
        id="cloudwatch",
    ),
    pytest.param("gs://bucket/path", {}, REMOTE_IO_PATHS["gs"], id="gs"),
    pytest.param("wasb://logs", {}, REMOTE_IO_PATHS["wasb"], id="wasb"),
    pytest.param("stackdriver://host/path", {}, REMOTE_IO_PATHS["stackdriver"], id="stackdriver"),
    pytest.param("oss://bucket/path", {}, REMOTE_IO_PATHS["oss"], id="oss"),
    pytest.param("hdfs://host/path", {}, REMOTE_IO_PATHS["hdfs"], id="hdfs"),
    pytest.param(
        "elasticsearch://es.example.com:9200",
        {("elasticsearch", "host"): "es.example.com:9200"},
        REMOTE_IO_PATHS["elasticsearch"],
        id="elasticsearch",
    ),
    pytest.param(
        "opensearch://os.example.com:9200",
        {("elasticsearch", "host"): "", ("opensearch", "host"): "os.example.com:9200"},
        REMOTE_IO_PATHS["opensearch"],
        id="opensearch",
    ),
]

UNDISPATCHABLE_BACKENDS = [
    pytest.param("wasb-logs", {}, REMOTE_IO_PATHS["wasb"], "wasb", id="wasb-bare-path"),
    pytest.param(
        "",
        {("elasticsearch", "host"): "es.example.com:9200"},
        REMOTE_IO_PATHS["elasticsearch"],
        "elasticsearch",
        id="elasticsearch-selected-by-host",
    ),
    pytest.param(
        "",
        {("elasticsearch", "host"): "", ("opensearch", "host"): "os.example.com:9200"},
        REMOTE_IO_PATHS["opensearch"],
        "opensearch",
        id="opensearch-selected-by-host",
    ),
]


class _PreDispatchRemoteLogIO:
    """Stand-in for a provider release predating ``from_config``, which registers no scheme."""

    def __init__(self, *args, **kwargs):
        pass


def _reload_with(remote_base, extra_conf):
    return conf_vars(
        {
            ("logging", "remote_logging"): "True",
            ("logging", "remote_base_log_folder"): remote_base,
            **extra_conf,
        }
    )


@pytest.mark.parametrize(("remote_base", "extra_conf", "remote_io_path"), DISPATCHABLE_BACKENDS)
def test_no_deprecation_warning_when_provider_dispatch_supersedes_branch(
    remote_base, extra_conf, remote_io_path, restore_local_settings
):
    """A current provider plus a scheme URL means ProvidersManager owns resolution: stay quiet."""
    pytest.importorskip(remote_io_path.rsplit(".", 1)[0])
    with (
        mock.patch(remote_io_path),
        _reload_with(remote_base, extra_conf),
        warnings.catch_warnings(),
    ):
        warnings.simplefilter("error", RemovedInAirflow4Warning)
        importlib.reload(airflow_local_settings)


@pytest.mark.parametrize(("remote_base", "extra_conf", "remote_io_path"), DISPATCHABLE_BACKENDS)
def test_warns_to_upgrade_provider_when_from_config_is_missing(
    remote_base, extra_conf, remote_io_path, restore_local_settings
):
    """A provider without ``from_config`` registers no scheme, so this branch is load-bearing."""
    pytest.importorskip(remote_io_path.rsplit(".", 1)[0])
    with (
        mock.patch(remote_io_path, new=_PreDispatchRemoteLogIO),
        _reload_with(remote_base, extra_conf),
        pytest.warns(RemovedInAirflow4Warning, match="Upgrade apache-airflow-providers-"),
    ):
        importlib.reload(airflow_local_settings)


@pytest.mark.parametrize(("remote_base", "extra_conf", "remote_io_path", "scheme"), UNDISPATCHABLE_BACKENDS)
def test_warns_to_set_scheme_url_when_dispatch_cannot_match(
    remote_base, extra_conf, remote_io_path, scheme, restore_local_settings
):
    """A current provider is not enough: without a scheme URL there is nothing to dispatch on."""
    pytest.importorskip(remote_io_path.rsplit(".", 1)[0])
    with (
        mock.patch(remote_io_path),
        _reload_with(remote_base, extra_conf),
        pytest.warns(RemovedInAirflow4Warning, match=f'remote_base_log_folder to a "{scheme}://" URL'),
    ):
        importlib.reload(airflow_local_settings)


def test_no_deprecation_warning_when_remote_logging_disabled(restore_local_settings):
    """The chain does not run at all without remote logging, so nothing may be deprecated."""
    with (
        conf_vars({("logging", "remote_logging"): "False"}),
        warnings.catch_warnings(),
    ):
        warnings.simplefilter("error", RemovedInAirflow4Warning)
        importlib.reload(airflow_local_settings)


def test_every_legacy_branch_has_a_documented_min_version():
    """Each scheme the chain can warn about must resolve to a distribution and version."""
    assert set(airflow_local_settings._PROVIDER_DISPATCH_MIN_VERSIONS) == {
        "s3",
        "cloudwatch",
        "gs",
        "stackdriver",
        "wasb",
        "oss",
        "hdfs",
        "elasticsearch",
        "opensearch",
    }
