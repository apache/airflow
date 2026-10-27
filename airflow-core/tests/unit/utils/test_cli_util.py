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

import argparse
import ast
import json
import os
import shutil
import sys
from argparse import Namespace
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

import pytest
from sqlalchemy import select

import airflow
from airflow import settings
from airflow._shared.timezones import timezone
from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.dag_processing.lang_sdk_processor import LangSDKDagFileProcessorProcess
from airflow.dag_processing.processor import DagFileParsingResult
from airflow.exceptions import AirflowException
from airflow.models.dag import DagModel
from airflow.models.dagbundle import DagBundleModel
from airflow.models.log import Log
from airflow.models.team import Team
from airflow.sdk import DAG, BaseOperator
from airflow.serialization.serialized_objects import DagSerialization, LazyDeserializedDAG
from airflow.utils import cli, cli_action_loggers
from airflow.utils.cli import _search_for_dag_file
from airflow.utils.session import create_session

from tests_common.test_utils import db
from tests_common.test_utils.config import conf_vars
from unit.dag_processing.fake_lang_sdk import fake_coordinator, write_native_file

# Mark entire module as db_test because ``action_cli`` wrapper still could use DB on callbacks:
# - ``cli_action_loggers.on_pre_execution``
# - ``cli_action_loggers.on_post_execution``
pytestmark = pytest.mark.db_test
repo_root = Path(airflow.__file__).parents[1]
MASKED = "*" * 8

_DEFERRED_BUNDLE_NAME = "deferred-path-bundle"
_DEFERRED_BUNDLE_DAG_ID = "deferred_path_bundle_dag"


class _DeferredPathBundle(BaseDagBundle):
    """Bundle whose files only land on disk in ``initialize()``, like the Git bundle's clone."""

    def __init__(self, *, source: Path, **kwargs) -> None:
        super().__init__(**kwargs)
        self._source = source
        self._path = source.parent / "checkout"

    def initialize(self) -> None:
        shutil.copytree(self._source, self._path, dirs_exist_ok=True)
        super().initialize()

    def get_current_version(self) -> None:
        return None

    def refresh(self) -> None:
        """Nothing to refresh."""

    @property
    def path(self) -> Path:
        return self._path


def _make_deferred_path_bundle(tmp_path: Path) -> _DeferredPathBundle:
    source = tmp_path / "source"
    source.mkdir()
    (source / "dag.py").write_text(
        f"from airflow.sdk import DAG\nwith DAG({_DEFERRED_BUNDLE_DAG_ID!r}, schedule=None):\n    pass\n"
    )
    return _DeferredPathBundle(name=_DEFERRED_BUNDLE_NAME, source=source)


class TestCliUtil:
    def test_metrics_build(self):
        func_name = "test"
        namespace = Namespace(dag_id="foo", task_id="bar", subcommand="test")
        metrics = cli._build_metrics(func_name, namespace)

        expected = {
            "user": os.environ.get("USER"),
            "sub_command": "test",
            "dag_id": "foo",
            "task_id": "bar",
        }
        for k, v in expected.items():
            assert v == metrics.get(k)

        assert metrics.get("start_datetime") <= timezone.utcnow()
        assert metrics.get("full_command")

    @pytest.mark.parametrize(
        ("func_name", "namespace", "expected_team_name"),
        [
            pytest.param("triggerer", Namespace(team_name="payments"), "payments", id="team-name-option"),
            pytest.param("team_create", Namespace(name="payments"), "payments", id="teams-positional"),
            pytest.param("team_list", Namespace(output="table"), None, id="no-team"),
            pytest.param("dag_list", Namespace(name="not-a-team"), None, id="name-of-something-else"),
        ],
    )
    def test_metrics_build_team_name(self, func_name, namespace, expected_team_name):
        assert cli._build_metrics(func_name, namespace).get("team_name") == expected_team_name

    def test_fail_function(self):
        """
        Actual function is failing and fail needs to be propagated.
        :return:
        """
        with pytest.raises(NotImplementedError):
            fail_func(Namespace())

    def test_success_function(self):
        """
        Test success function but with failing callback.
        In this case, failure should not propagate.
        :return:
        """
        with fail_action_logger_callback():
            success_func(Namespace())

    def test_process_subdir_path_with_placeholder(self):
        assert os.path.join(settings.DAGS_FOLDER, "abc") == cli.process_subdir("DAGS_FOLDER/abc")

    def test_get_dags(self):
        dags = cli.get_dags(None, "test_example_bash_operator")
        assert len(dags) == 1

        with pytest.raises(AirflowException):
            cli.get_dags(None, "foobar", True)

    @pytest.mark.parametrize(
        "get_matching_dags",
        [
            pytest.param(
                lambda: cli.get_bagged_dag([_DEFERRED_BUNDLE_NAME], _DEFERRED_BUNDLE_DAG_ID),
                id="get_bagged_dag",
            ),
            pytest.param(
                lambda: cli.get_dags([_DEFERRED_BUNDLE_NAME], f"^{_DEFERRED_BUNDLE_DAG_ID}$", use_regex=True)[
                    0
                ],
                id="get_dags_regex",
            ),
        ],
    )
    @mock.patch("airflow.utils.cli.DagBundlesManager", autospec=True)
    def test_explicit_bundle_name_is_initialized(self, mock_manager_cls, tmp_path, get_matching_dags):
        bundle = _make_deferred_path_bundle(tmp_path)
        mock_manager_cls.return_value.get_bundle.return_value = bundle
        mock_manager_cls.return_value.get_all_dag_bundles.return_value = []

        assert get_matching_dags().dag_id == _DEFERRED_BUNDLE_DAG_ID
        assert bundle.is_initialized

    @pytest.mark.parametrize(
        ("given_command", "expected_masked_command", "is_command_list"),
        [
            (
                "airflow users create -u test2 -l doe -f jon -e jdoe@apache.org -r admin --password test",
                "airflow users create -u test2 -l doe -f jon -e jdoe@apache.org -r admin --password ********",
                False,
            ),
            (
                "airflow users create -u test2 -l doe -f jon -e jdoe@apache.org -r admin -p test",
                "airflow users create -u test2 -l doe -f jon -e jdoe@apache.org -r admin -p ********",
                False,
            ),
            (
                "airflow users create -u test2 -l doe -f jon -e jdoe@apache.org -r admin --password=test",
                "airflow users create -u test2 -l doe -f jon -e jdoe@apache.org -r admin --password=********",
                False,
            ),
            (
                "airflow users create -u test2 -l doe -f jon -e jdoe@apache.org -r admin -p=test",
                "airflow users create -u test2 -l doe -f jon -e jdoe@apache.org -r admin -p=********",
                False,
            ),
            (
                "airflow connections add dsfs --conn-login asd --conn-password test --conn-type google",
                "airflow connections add dsfs --conn-login asd --conn-password ******** --conn-type google",
                False,
            ),
            (
                "airflow connections add my_postgres_conn --conn-uri postgresql://user:my-password@localhost:5432/mydatabase",
                "airflow connections add my_postgres_conn --conn-uri postgresql://user:********@localhost:5432/mydatabase",
                False,
            ),
            (
                [
                    "airflow",
                    "connections",
                    "add",
                    "my_new_conn",
                    "--conn-json",
                    '{"conn_type": "my-conn-type", "login": "my-login", "password": "my-password", "host": "my-host", "port": 1234, "schema": "my-schema", "extra": {"param1": "val1", "param2": "val2"}}',
                ],
                [
                    "airflow",
                    "connections",
                    "add",
                    "my_new_conn",
                    "--conn-json",
                    '{"conn_type": "my-conn-type", "login": "my-login", "password": "********", '
                    '"host": "my-host", "port": 1234, "schema": "my-schema", "extra": {"param1": '
                    '"val1", "param2": "val2"}}',
                ],
                True,
            ),
            (
                [
                    "airflow",
                    "connections",
                    "add",
                    "tp005-demo",
                    "--conn-type",
                    "generic",
                    "--conn-extra",
                    '{"nested": {"password": "TP005_CONN_DUMMY"}, "region": "eu-west-1"}',
                ],
                [
                    "airflow",
                    "connections",
                    "add",
                    "tp005-demo",
                    "--conn-type",
                    "generic",
                    "--conn-extra",
                    '{"nested": {"password": "********"}, "region": "eu-west-1"}',
                ],
                True,
            ),
            (
                [
                    "airflow",
                    "connections",
                    "add",
                    "nested_conn",
                    "--conn-json",
                    '{"conn_type": "generic", "extra": {"auth": {"password": "deep-secret"}}}',
                ],
                [
                    "airflow",
                    "connections",
                    "add",
                    "nested_conn",
                    "--conn-json",
                    '{"conn_type": "generic", "extra": {"auth": {"password": "********"}}}',
                ],
                True,
            ),
            # argparse accepts "--option=VALUE" too; the audit masker must cover both
            # spellings or a supported invocation records the secret in clear.
            (
                [
                    "airflow",
                    "connections",
                    "add",
                    "eq_json_conn",
                    '--conn-json={"conn_type": "generic", "password": "eq-secret", '
                    '"extra": {"auth": {"api_key": "deep-eq-secret"}}}',
                ],
                [
                    "airflow",
                    "connections",
                    "add",
                    "eq_json_conn",
                    '--conn-json={"conn_type": "generic", "password": "********", '
                    '"extra": {"auth": {"api_key": "********"}}}',
                ],
                True,
            ),
            (
                [
                    "airflow",
                    "connections",
                    "add",
                    "eq_extra_conn",
                    "--conn-type",
                    "generic",
                    '--conn-extra={"nested": {"password": "eq-extra-secret"}}',
                ],
                [
                    "airflow",
                    "connections",
                    "add",
                    "eq_extra_conn",
                    "--conn-type",
                    "generic",
                    '--conn-extra={"nested": {"password": "********"}}',
                ],
                True,
            ),
            (
                "airflow connections add eq_uri_conn --conn-uri=postgresql://user:my-password@localhost:5432/db",
                "airflow connections add eq_uri_conn --conn-uri=postgresql://user:********@localhost:5432/db",
                False,
            ),
            (
                "airflow scheduler -p",
                "airflow scheduler -p",
                False,
            ),
            (
                "airflow celery flower -p 8888",
                "airflow celery flower -p 8888",
                False,
            ),
            (
                "airflow keycloak-auth-manager create-scopes --username admin --password s3cr3t",
                "airflow keycloak-auth-manager create-scopes --username admin --password ********",
                False,
            ),
            (
                "airflow keycloak-auth-manager create-scopes --username admin --password=s3cr3t",
                "airflow keycloak-auth-manager create-scopes --username admin --password=********",
                False,
            ),
            (
                "airflow api-server -p 8080",
                "airflow api-server -p 8080",
                False,
            ),
        ],
    )
    def test_cli_create_user_supplied_password_is_masked(
        self, given_command, expected_masked_command, is_command_list, session
    ):
        # '-p' value which is not password, like 'airflow scheduler -p'
        # or 'airflow celery flower -p 8888', should not be masked
        args = given_command if is_command_list else given_command.split()
        expected_command = expected_masked_command if is_command_list else expected_masked_command.split()

        exec_date = timezone.utcnow()
        namespace = Namespace(dag_id="foo", task_id="bar", subcommand="test", logical_date=exec_date)
        with (
            mock.patch.object(sys, "argv", args),
            mock.patch("airflow.utils.session.create_session") as mock_create_session,
        ):
            metrics = cli._build_metrics(args[1], namespace)
            # Make it so the default_action_log doesn't actually commit the txn, by giving it a next txn
            # instead
            mock_create_session.return_value = session.begin_nested()
            mock_create_session.return_value.bulk_insert_mappings = session.bulk_insert_mappings
            cli_action_loggers.default_action_log(**metrics)

            log = session.scalar(select(Log).order_by(Log.dttm.desc()))

        assert metrics.get("start_datetime") <= timezone.utcnow()

        command: str = json.loads(log.extra).get("full_command")
        # Replace single quotes to double quotes to avoid json decode error
        command = ast.literal_eval(command)
        assert command == expected_command

    @pytest.mark.parametrize(
        ("given_command", "parsed", "expected_command"),
        [
            pytest.param(
                ["airflow", "users", "create", "-u", "a", "-r", "Admin", "-pS3CRET"],
                {"username": "a", "role": "Admin", "password": "S3CRET"},
                ["airflow", "users", "create", "-u", "a", "-r", "Admin", f"-p{MASKED}"],
                id="users-short-option-attached",
            ),
            pytest.param(
                ["airflow", "users", "create", "-u", "a", "--pass", "S3CRET"],
                {"username": "a", "password": "S3CRET"},
                ["airflow", "users", "create", "-u", "a", "--pass", MASKED],
                id="users-abbreviated",
            ),
            pytest.param(
                ["airflow", "users", "reset-password", "-u", "a", "--passw=S3CRET"],
                {"username": "a", "password": "S3CRET"},
                ["airflow", "users", "reset-password", "-u", "a", f"--passw={MASKED}"],
                id="users-abbreviated-joined",
            ),
            pytest.param(
                ["airflow", "users", "create", "-u", "a", "--p", "S3CRET"],
                {"username": "a", "password": "S3CRET"},
                ["airflow", "users", "create", "-u", "a", "--p", MASKED],
                id="users-shortest-abbreviation",
            ),
            pytest.param(
                [
                    "airflow",
                    "keycloak-auth-manager",
                    "create-scopes",
                    "--username",
                    "admin",
                    "--pass",
                    "S3CRET",
                ],
                {"username": "admin", "password": "S3CRET"},
                [
                    "airflow",
                    "keycloak-auth-manager",
                    "create-scopes",
                    "--username",
                    "admin",
                    "--pass",
                    MASKED,
                ],
                id="keycloak-abbreviated",
            ),
            pytest.param(
                ["airflow", "keycloak-auth-manager", "create-all", "--username", "admin", "--p=S3CRET"],
                {"username": "admin", "password": "S3CRET"},
                ["airflow", "keycloak-auth-manager", "create-all", "--username", "admin", f"--p={MASKED}"],
                id="keycloak-abbreviated-joined",
            ),
            pytest.param(
                ["airflow", "connections", "add", "c1", "--conn-type", "generic", "--conn-pass", "S3CRET"],
                {"conn_id": "c1", "conn_type": "generic", "conn_password": "S3CRET"},
                ["airflow", "connections", "add", "c1", "--conn-type", "generic", "--conn-pass", MASKED],
                id="conn-password-abbreviated",
            ),
            pytest.param(
                ["airflow", "connections", "add", "c1", "--conn-type", "generic", "--conn-pa=S3CRET"],
                {"conn_id": "c1", "conn_type": "generic", "conn_password": "S3CRET"},
                ["airflow", "connections", "add", "c1", "--conn-type", "generic", f"--conn-pa={MASKED}"],
                id="conn-password-abbreviated-joined",
            ),
            pytest.param(
                ["airflow", "connections", "add", "c1", "--conn-ext", '{"password": "S3CRET"}'],
                {"conn_id": "c1", "conn_extra": '{"password": "S3CRET"}'},
                ["airflow", "connections", "add", "c1", "--conn-ext", f'{{"password": "{MASKED}"}}'],
                id="conn-extra-abbreviated",
            ),
            pytest.param(
                [
                    "airflow",
                    "connections",
                    "add",
                    "c1",
                    "--conn-j",
                    '{"conn_type": "generic", "password": "S3CRET"}',
                ],
                {"conn_id": "c1", "conn_json": '{"conn_type": "generic", "password": "S3CRET"}'},
                [
                    "airflow",
                    "connections",
                    "add",
                    "c1",
                    "--conn-j",
                    f'{{"conn_type": "generic", "password": "{MASKED}"}}',
                ],
                id="conn-json-abbreviated",
            ),
            pytest.param(
                ["airflow", "connections", "add", "c1", "--conn-u", "postgres://u:S3CRET@h/db"],
                {"conn_id": "c1", "conn_uri": "postgres://u:S3CRET@h/db"},
                ["airflow", "connections", "add", "c1", "--conn-u", f"postgres://u:{MASKED}@h/db"],
                id="conn-uri-abbreviated",
            ),
            pytest.param(
                [
                    "airflow",
                    "connections",
                    "add",
                    "c1",
                    "--conn-json",
                    '{"extra": "{\\"password\\": \\"S3CRET\\"}"}',
                ],
                {"conn_id": "c1", "conn_json": '{"extra": "{\\"password\\": \\"S3CRET\\"}"}'},
                [
                    "airflow",
                    "connections",
                    "add",
                    "c1",
                    "--conn-json",
                    f'{{"extra": "{{\\"password\\": \\"{MASKED}\\"}}"}}',
                ],
                id="conn-json-extra-as-json-string",
            ),
            pytest.param(
                ["airflow", "connections", "add", "c1", "--conn-uri", "http://h/?api_key=S3CRET&region=eu"],
                {"conn_id": "c1", "conn_uri": "http://h/?api_key=S3CRET&region=eu"},
                [
                    "airflow",
                    "connections",
                    "add",
                    "c1",
                    "--conn-uri",
                    f"http://h/?api_key={MASKED}&region=eu",
                ],
                id="conn-uri-query-parameter",
            ),
            pytest.param(
                [
                    "airflow",
                    "connections",
                    "add",
                    "c1",
                    "--conn-uri",
                    "http://h/?__extra__=%7B%22token%22%3A%22S3CRET%22%7D",
                ],
                {"conn_id": "c1", "conn_uri": "http://h/?__extra__=%7B%22token%22%3A%22S3CRET%22%7D"},
                [
                    "airflow",
                    "connections",
                    "add",
                    "c1",
                    "--conn-uri",
                    f"http://h/?__extra__=%7B%22token%22%3A+%22{MASKED}%22%7D",
                ],
                id="conn-uri-encoded-extra",
            ),
            pytest.param(
                ["airflow", "connections", "add", "c1", "--conn-uri", "aws://AKIA:S3CRET@"],
                {"conn_id": "c1", "conn_uri": "aws://AKIA:S3CRET@"},
                ["airflow", "connections", "add", "c1", "--conn-uri", f"aws://AKIA:{MASKED}@"],
                id="conn-uri-without-host",
            ),
            pytest.param(
                [
                    "airflow",
                    "connections",
                    "add",
                    "c1",
                    "--conn-uri",
                    "postgres://u:FIRST@h/db",
                    "--conn-uri",
                    "postgres://u:S3CRET@h/db",
                ],
                {"conn_id": "c1", "conn_uri": "postgres://u:S3CRET@h/db"},
                [
                    "airflow",
                    "connections",
                    "add",
                    "c1",
                    "--conn-uri",
                    f"postgres://u:{MASKED}@h/db",
                    "--conn-uri",
                    f"postgres://u:{MASKED}@h/db",
                ],
                id="conn-uri-repeated",
            ),
            pytest.param(
                [
                    "airflow",
                    "connections",
                    "add",
                    "c1",
                    "--conn-extra",
                    '{"password": "FIRST"}',
                    "--conn-extra",
                    '{"password": "S3CRET"}',
                ],
                {"conn_id": "c1", "conn_extra": '{"password": "S3CRET"}'},
                [
                    "airflow",
                    "connections",
                    "add",
                    "c1",
                    "--conn-extra",
                    f'{{"password": "{MASKED}"}}',
                    "--conn-extra",
                    f'{{"password": "{MASKED}"}}',
                ],
                id="conn-extra-repeated",
            ),
            pytest.param(
                ["airflow", "celery", "flower", "--basic-auth", "u:S3CRET"],
                {"basic_auth": "u:S3CRET", "port": 5555},
                ["airflow", "celery", "flower", "--basic-auth", MASKED],
                id="flower-basic-auth",
            ),
            pytest.param(
                ["airflow", "celery", "flower", "-A", "u:S3CRET"],
                {"basic_auth": "u:S3CRET", "port": 5555},
                ["airflow", "celery", "flower", "-A", MASKED],
                id="flower-basic-auth-short",
            ),
            pytest.param(
                ["airflow", "celery", "flower", "--broker-api", "http://u:S3CRET@rabbit:15672/api/"],
                {"broker_api": "http://u:S3CRET@rabbit:15672/api/", "port": 5555},
                ["airflow", "celery", "flower", "--broker-api", f"http://u:{MASKED}@rabbit:15672/api/"],
                id="flower-broker-api",
            ),
            pytest.param(
                ["airflow", "dags", "trigger", "d1", "--conf", '{"password": "S3CRET", "x": 1}'],
                {"dag_id": "d1", "conf": '{"password": "S3CRET", "x": 1}'},
                ["airflow", "dags", "trigger", "d1", "--conf", f'{{"password": "{MASKED}", "x": 1}}'],
                id="dags-trigger-conf",
            ),
            pytest.param(
                ["airflow", "dags", "trigger", "d1", '-c{"api_key": "S3CRET"}'],
                {"dag_id": "d1", "conf": '{"api_key": "S3CRET"}'},
                ["airflow", "dags", "trigger", "d1", f'-c{{"api_key": "{MASKED}"}}'],
                id="dags-trigger-conf-short-attached",
            ),
            pytest.param(
                ["airflow", "backfill", "create", "--dag-id", "d1", "--dag-run-conf", '{"token": "S3CRET"}'],
                {"dag_id": "d1", "dag_run_conf": '{"token": "S3CRET"}'},
                [
                    "airflow",
                    "backfill",
                    "create",
                    "--dag-id",
                    "d1",
                    "--dag-run-conf",
                    f'{{"token": "{MASKED}"}}',
                ],
                id="backfill-dag-run-conf",
            ),
            pytest.param(
                ["airflow", "users", "create", "-u", "a", "-vpS3CRET"],
                {"username": "a", "verbose": True, "password": "S3CRET"},
                ["airflow", "users", "create", "-u", "a", "-vp********"],
                id="grouped-short-flags",
            ),
            pytest.param(
                ["airflow", "tasks", "test", "d1", "t1", "-mt", '{"password": "S3CRET"}'],
                {
                    "dag_id": "d1",
                    "task_id": "t1",
                    "post_mortem": True,
                    "task_params": '{"password": "S3CRET"}',
                },
                ["airflow", "tasks", "test", "d1", "t1", "-mt", '{"password": "********"}'],
                id="grouped-short-flags-json-separate",
            ),
            pytest.param(
                ["airflow", "tasks", "test", "d1", "t1", '-mt{"password": "S3CRET"}'],
                {
                    "dag_id": "d1",
                    "task_id": "t1",
                    "post_mortem": True,
                    "task_params": '{"password": "S3CRET"}',
                },
                ["airflow", "tasks", "test", "d1", "t1", '-mt{"password": "********"}'],
                id="grouped-short-flags-json-attached",
            ),
            pytest.param(
                ["airflow", "tasks", "test", "d1", "t1", "-t", '{"password": "S3CRET"}'],
                {"dag_id": "d1", "task_id": "t1", "task_params": '{"password": "S3CRET"}'},
                ["airflow", "tasks", "test", "d1", "t1", "-t", '{"password": "********"}'],
                id="tasks-test-task-params",
            ),
            pytest.param(
                ["airflow", "tasks", "test", "d1", "t1", "--env-v", '{"API_TOKEN": "S3CRET"}'],
                {"dag_id": "d1", "task_id": "t1", "env_vars": {"API_TOKEN": "S3CRET"}},
                ["airflow", "tasks", "test", "d1", "t1", "--env-v", '{"API_TOKEN": "********"}'],
                id="tasks-test-env-vars-abbreviated",
            ),
            pytest.param(
                ["airflow", "users", "reset-password", "-u", "a", "-vpFIRST", "-vpS3CRET"],
                {"username": "a", "verbose": True, "password": "S3CRET"},
                ["airflow", "users", "reset-password", "-u", "a", f"-vp{MASKED}", f"-vp{MASKED}"],
                id="grouped-short-flags-repeated",
            ),
            pytest.param(
                ["airflow", "users", "reset-password", "-u", "a", "-vp", "FIRST", "-p", "S3CRET"],
                {"username": "a", "verbose": True, "password": "S3CRET"},
                ["airflow", "users", "reset-password", "-u", "a", "-vp", MASKED, "-p", MASKED],
                id="grouped-short-flags-separate-value-repeated",
            ),
            pytest.param(
                ["airflow", "users", "create", "-uapple", "-pS3CRET"],
                {"username": "apple", "password": "S3CRET"},
                ["airflow", "users", "create", "-uapple", f"-p{MASKED}"],
                id="short-option-value-containing-secret-option-letter",
            ),
            pytest.param(
                ["airflow", "users", "create", "-u", "a", "--pass", "FIRST", "--password", ""],
                {"username": "a", "password": ""},
                ["airflow", "users", "create", "-u", "a", "--pass", MASKED, "--password", MASKED],
                id="password-repeated-last-empty",
            ),
            pytest.param(
                [
                    "airflow",
                    "tasks",
                    "test",
                    "d1",
                    "t1",
                    "--env-v",
                    '{"API_TOKEN": "FIRST"}',
                    "--env-v",
                    "{}",
                ],
                {"dag_id": "d1", "task_id": "t1", "env_vars": {}},
                [
                    "airflow",
                    "tasks",
                    "test",
                    "d1",
                    "t1",
                    "--env-v",
                    f'{{"API_TOKEN": "{MASKED}"}}',
                    "--env-v",
                    "{}",
                ],
                id="env-vars-repeated-last-empty",
            ),
            pytest.param(
                ["airflow", "connections", "add", "c1", "--conn-uri", "http://https://u:S3CRET@h:443/p"],
                {"conn_id": "c1", "conn_uri": "http://https://u:S3CRET@h:443/p"},
                ["airflow", "connections", "add", "c1", "--conn-uri", f"http://https://u:{MASKED}@h:443/p"],
                id="conn-uri-protocol-in-host",
            ),
            pytest.param(
                ["airflow", "connections", "add", "c1", "--conn-uri", "postgres://u:S3CRET@h/db?x=http://y"],
                {"conn_id": "c1", "conn_uri": "postgres://u:S3CRET@h/db?x=http://y"},
                [
                    "airflow",
                    "connections",
                    "add",
                    "c1",
                    "--conn-uri",
                    f"postgres://u:{MASKED}@h/db?x=http://y",
                ],
                id="conn-uri-url-in-query",
            ),
            pytest.param(
                [
                    "airflow",
                    "connections",
                    "add",
                    "c1",
                    "--conn-uri",
                    "http://https://u:S3CRET@h/p?next=http://z",
                ],
                {"conn_id": "c1", "conn_uri": "http://https://u:S3CRET@h/p?next=http://z"},
                [
                    "airflow",
                    "connections",
                    "add",
                    "c1",
                    "--conn-uri",
                    f"http://https://u:{MASKED}@h/p?next=http://z",
                ],
                id="conn-uri-protocol-in-host-and-url-in-query",
            ),
            pytest.param(
                ["airflow", "connections", "add", "c1", "--conn-uri", "http:// https://u:S3CRET@h"],
                {"conn_id": "c1", "conn_uri": "http:// https://u:S3CRET@h"},
                ["airflow", "connections", "add", "c1", "--conn-uri", f"http://https://u:{MASKED}@h"],
                id="conn-uri-protocol-in-host-after-space",
            ),
            pytest.param(
                ["airflow", "connections", "add", "c1", "--conn-uri", "http://\thttps://u:S3CRET@h"],
                {"conn_id": "c1", "conn_uri": "http://\thttps://u:S3CRET@h"},
                ["airflow", "connections", "add", "c1", "--conn-uri", f"http://https://u:{MASKED}@h"],
                id="conn-uri-protocol-in-host-after-tab",
            ),
            pytest.param(
                ["airflow", "connections", "add", "c1", "--conn-uri", "postgres://u:S3CRET/x@h/db"],
                {"conn_id": "c1", "conn_uri": "postgres://u:S3CRET/x@h/db"},
                ["airflow", "connections", "add", "c1", "--conn-uri", MASKED],
                id="conn-uri-unparsed-credentials",
            ),
            pytest.param(
                ["airflow", "connections", "add", "c1", "--conn-uri", "http://u:pw@h/db#u:S3CRET@h2"],
                {"conn_id": "c1", "conn_uri": "http://u:pw@h/db#u:S3CRET@h2"},
                ["airflow", "connections", "add", "c1", "--conn-uri", MASKED],
                id="conn-uri-credentials-in-fragment",
            ),
            pytest.param(
                ["airflow", "connections", "add", "c1", "--conn-uri", "http://h/db?target=u:S3CRET@h2"],
                {"conn_id": "c1", "conn_uri": "http://h/db?target=u:S3CRET@h2"},
                ["airflow", "connections", "add", "c1", "--conn-uri", MASKED],
                id="conn-uri-credentials-in-query-value",
            ),
            pytest.param(
                ["airflow", "connections", "add", "c1", "--conn-uri", "postgres://u:S3CRET?tail@h/db"],
                {"conn_id": "c1", "conn_uri": "postgres://u:S3CRET?tail@h/db"},
                ["airflow", "connections", "add", "c1", "--conn-uri", MASKED],
                id="conn-uri-password-split-by-question-mark",
            ),
            pytest.param(
                ["airflow", "connections", "add", "c1", "--conn-uri", "postgres://u:S3CRET#tail@h/db"],
                {"conn_id": "c1", "conn_uri": "postgres://u:S3CRET#tail@h/db"},
                ["airflow", "connections", "add", "c1", "--conn-uri", MASKED],
                id="conn-uri-password-split-by-hash",
            ),
            pytest.param(
                ["airflow", "connections", "add", "c1", "--conn-uri", "postgres://h/db?x=u%3AS3CRET%40h2"],
                {"conn_id": "c1", "conn_uri": "postgres://h/db?x=u%3AS3CRET%40h2"},
                ["airflow", "connections", "add", "c1", "--conn-uri", MASKED],
                id="conn-uri-percent-encoded-credentials-in-query",
            ),
            pytest.param(
                [
                    "airflow",
                    "connections",
                    "add",
                    "c1",
                    "--conn-uri",
                    "http://h/db?target=u%3AS3CRET%0A%40h2",
                ],
                {"conn_id": "c1", "conn_uri": "http://h/db?target=u%3AS3CRET%0A%40h2"},
                ["airflow", "connections", "add", "c1", "--conn-uri", MASKED],
                id="conn-uri-percent-encoded-newline-in-credentials",
            ),
            pytest.param(
                ["airflow", "connections", "add", "c1", "--conn-uri", "http://h/db?target=u:S3CRET\r\n@h2"],
                {"conn_id": "c1", "conn_uri": "http://h/db?target=u:S3CRET\r\n@h2"},
                ["airflow", "connections", "add", "c1", "--conn-uri", MASKED],
                id="conn-uri-raw-newline-in-credentials",
            ),
            pytest.param(
                [
                    "airflow",
                    "connections",
                    "add",
                    "c1",
                    "--conn-uri",
                    "http://h/?__extra__=%7B%22token%22%3A%22S3CRET%22%2C%22email%22%3A%22a%40b.c%22%7D",
                ],
                {
                    "conn_id": "c1",
                    "conn_uri": "http://h/?__extra__=%7B%22token%22%3A%22S3CRET%22%2C%22email%22%3A%22a%40b.c%22%7D",
                },
                ["airflow", "connections", "add", "c1", "--conn-uri", MASKED],
                id="conn-uri-encoded-extra-with-email",
            ),
            pytest.param(
                ["airflow", "users", "reset-password", "-u", "p", "-vp", "FIRST", "-p", "S3CRET"],
                {"username": "p", "verbose": True, "password": "S3CRET"},
                ["airflow", "users", "reset-password", "-u", "p", "-vp", MASKED, "-p", MASKED],
                id="grouped-short-flags-username-equals-cluster-tail",
            ),
            pytest.param(
                ["airflow", "users", "reset-password", "-u", "p", "-vpFIRST", "-p", "S3CRET"],
                {"username": "p", "verbose": True, "password": "S3CRET"},
                ["airflow", "users", "reset-password", "-u", "p", f"-vp{MASKED}", "-p", MASKED],
                id="grouped-short-flags-attached-username-p",
            ),
            pytest.param(
                [
                    "airflow",
                    "connections",
                    "add",
                    "c1",
                    "--conn-uri",
                    "http://h/?nested=%7B%22password%22%3A%22S3CRET%22%7D",
                ],
                {"conn_id": "c1", "conn_uri": "http://h/?nested=%7B%22password%22%3A%22S3CRET%22%7D"},
                [
                    "airflow",
                    "connections",
                    "add",
                    "c1",
                    "--conn-uri",
                    f"http://h/?nested=%7B%22password%22%3A+%22{MASKED}%22%7D",
                ],
                id="conn-uri-json-query-parameter",
            ),
            # ``-p`` that does not parse into a password is recorded as given.
            pytest.param(
                ["airflow", "api-server", "-p", "8080"],
                {"port": 8080},
                ["airflow", "api-server", "-p", "8080"],
                id="api-server-port",
            ),
            pytest.param(
                ["airflow", "roles", "list", "-p"],
                {"permission": True},
                ["airflow", "roles", "list", "-p"],
                id="roles-list-permission",
            ),
            pytest.param(
                ["airflow", "celery", "flower", "-p", "5555"],
                {"port": 5555, "basic_auth": None, "broker_api": None},
                ["airflow", "celery", "flower", "-p", "5555"],
                id="flower-port",
            ),
            pytest.param(
                ["airflow", "dags", "trigger", "d1", "--conf", '{"retries": 3}'],
                {"dag_id": "d1", "conf": '{"retries": 3}'},
                ["airflow", "dags", "trigger", "d1", "--conf", '{"retries": 3}'],
                id="dags-trigger-conf-without-secrets",
            ),
        ],
    )
    def test_secrets_masked_in_any_argument_spelling(self, given_command, parsed, expected_command):
        """Mask secrets supplied through any supported argument spelling in the recorded command line."""
        with mock.patch.object(sys, "argv", given_command):
            metrics = cli._build_metrics(given_command[1], Namespace(**parsed))

        command = ast.literal_eval(metrics["full_command"])
        assert command == expected_command
        assert not any("S3CRET" in argument or "FIRST" in argument for argument in command)

    def test_short_secret_is_not_masked_inside_other_arguments(self):
        """A short secret only masks the option value, not every token containing its text."""
        command = ["airflow", "users", "create", "-u", "alice", "-r", "Admin", "-p", "a"]
        with mock.patch.object(sys, "argv", command):
            metrics = cli._build_metrics("users", Namespace(username="alice", role="Admin", password="a"))

        assert ast.literal_eval(metrics["full_command"]) == [
            "airflow",
            "users",
            "create",
            "-u",
            "alice",
            "-r",
            "Admin",
            "-p",
            "********",
        ]

    @mock.patch.object(cli, "_mask_parsed_arguments", side_effect=RuntimeError("cannot mask S3CRET"))
    @mock.patch.object(cli_action_loggers, "on_pre_execution", autospec=True)
    def test_masking_failure_does_not_abort_the_command_or_leak_the_secret(
        self, mock_on_pre_execution, _mock_mask
    ):
        command = ["airflow", "users", "create", "-u", "alice", "--password", "S3CRET"]
        with mock.patch.object(sys, "argv", command):
            assert returns_ran(Namespace(username="alice", password="S3CRET")) == "ran"

        recorded = mock_on_pre_execution.call_args.kwargs["full_command"]
        assert ast.literal_eval(recorded) == ["airflow", "********"]
        assert "S3CRET" not in recorded

    def test_short_option_table_matches_the_parsers(self):
        """The short-option table must follow the command definitions, or masking drifts silently."""
        from airflow.cli import cli_parser

        def get_subparsers(parser):
            for action in parser._actions:
                if isinstance(action, argparse._SubParsersAction):
                    return action.choices
            return {}

        masked_dests = {*cli._SECRET_OPTIONS, *cli._JSON_OPTIONS, *cli._URI_OPTIONS}
        groups = get_subparsers(cli_parser.get_parser())
        actual = {}
        for group, group_parser in groups.items():
            for command, command_parser in get_subparsers(group_parser).items():
                short = {
                    option[1]: action
                    for action in command_parser._actions
                    for option in action.option_strings
                    if len(option) == 2 and option != "-h"
                }
                if any(action.dest in masked_dests for action in short.values()):
                    flags = "".join(sorted(o for o, a in short.items() if a.nargs == 0))
                    values = "".join(sorted(o for o, a in short.items() if a.nargs != 0))
                    actual[(group, command)] = (flags, values)

        # Provider command groups (``users``, ``celery``) only exist when the provider is installed.
        expected = {
            key: ("".join(sorted(flags)), "".join(sorted(values)))
            for key, (flags, values) in cli._SHORT_OPTION_ARITY.items()
            if key[0] in groups
        }
        assert expected == actual

    def test_action_log_records_team_name(self, session):
        namespace = Namespace(team_name="payments")
        with (
            mock.patch.object(sys, "argv", ["airflow", "triggerer", "--team-name", "payments"]),
            mock.patch("airflow.utils.session.create_session") as mock_create_session,
        ):
            metrics = cli._build_metrics("triggerer", namespace)
            mock_create_session.return_value = session.begin_nested()
            mock_create_session.return_value.bulk_insert_mappings = session.bulk_insert_mappings
            cli_action_loggers.default_action_log(**metrics)

            log = session.scalar(select(Log).order_by(Log.dttm.desc()))

        assert log.event == "cli_triggerer"
        assert log.dag_id is None
        assert log.team_name == "payments"

    @conf_vars({("core", "multi_team"): "True"})
    def test_action_log_records_the_team_owning_the_dag(self, session):
        bundle = DagBundleModel(name="team-bundle")
        bundle.teams.append(Team(name="payments"))
        session.add(bundle)
        session.flush()
        session.add(DagModel(dag_id="dag_owned_by_a_team", bundle_name="team-bundle", is_stale=False))
        session.flush()

        namespace = Namespace(dag_id="dag_owned_by_a_team", subcommand="pause")
        with (
            mock.patch.object(sys, "argv", ["airflow", "dags", "pause", "dag_owned_by_a_team"]),
            mock.patch("airflow.utils.session.create_session") as mock_create_session,
        ):
            metrics = cli._build_metrics("dag_pause", namespace)
            mock_create_session.return_value = session.begin_nested()
            mock_create_session.return_value.bulk_insert_mappings = session.bulk_insert_mappings
            mock_create_session.return_value.scalar = session.scalar
            cli_action_loggers.default_action_log(**metrics)

            log = session.scalar(select(Log).where(Log.dag_id == "dag_owned_by_a_team"))

        assert log.team_name == "payments"

    def test_dag_cli_records_the_dag_id_in_the_audit_log(self):
        """``DAG.cli()``'s parser drops ``dag_id``, so the Dag has to supply it before dispatch."""
        dag = DAG("dag_cli_audit_log")

        with (
            mock.patch.object(sys, "argv", ["dag_cli_audit_log.py", "tasks", "list"]),
            mock.patch("airflow.utils.session.create_session") as mock_create_session,
        ):
            dag.cli()

        audit_session = mock_create_session.return_value.__enter__.return_value
        model, rows = audit_session.bulk_insert_mappings.call_args.args
        assert model is Log
        assert rows[0]["event"] == "cli_task_list"
        assert rows[0]["dag_id"] == "dag_cli_audit_log"

    def test_setup_locations_relative_pid_path(self):
        relative_pid_path = "fake.pid"
        pid_full_path = os.path.join(os.getcwd(), relative_pid_path)
        pid, _, _, _ = cli.setup_locations(process="fake_process", pid=relative_pid_path)
        assert pid == pid_full_path

    def test_setup_locations_absolute_pid_path(self):
        abs_pid_path = os.path.join(os.getcwd(), "fake.pid")
        pid, _, _, _ = cli.setup_locations(process="fake_process", pid=abs_pid_path)
        assert pid == abs_pid_path

    def test_setup_locations_none_pid_path(self):
        process_name = "fake_process"
        default_pid_path = os.path.join(settings.AIRFLOW_HOME, f"airflow-{process_name}.pid")
        pid, _, _, _ = cli.setup_locations(process=process_name)
        assert pid == default_pid_path

    @pytest.mark.parametrize(
        ("given_command", "expected_masked_command", "var_key", "var_value"),
        [
            (
                "airflow variables set --description 'needed for dag 4' client_secret_234 7fh4375f5gy353wdf",
                "airflow variables set --description 'needed for dag 4' client_secret_234 ********",
                "client_secret_234",
                "7fh4375f5gy353wdf",
            ),
            (
                "airflow variables set cust_secret_234 7fh4375f5gy353wdf",
                "airflow variables set cust_secret_234 ********",
                "cust_secret_234",
                "7fh4375f5gy353wdf",
            ),
            # A trailing option displaces the value out of the last position. The masker
            # used to key off argv positions and recorded the value in clear here.
            (
                "airflow variables set client_secret_234 7fh4375f5gy353wdf --description validation",
                "airflow variables set client_secret_234 ******** --description validation",
                "client_secret_234",
                "7fh4375f5gy353wdf",
            ),
            (
                "airflow variables set client_secret_234 7fh4375f5gy353wdf --serialize-json",
                "airflow variables set client_secret_234 ******** --serialize-json",
                "client_secret_234",
                "7fh4375f5gy353wdf",
            ),
            # A non-sensitive key must still be recorded as given.
            (
                "airflow variables set retries 5 --description validation",
                "airflow variables set retries 5 --description validation",
                "retries",
                "5",
            ),
        ],
    )
    def test_cli_set_variable_supplied_sensitive_value_is_masked(
        self, given_command, expected_masked_command, var_key, var_value, session
    ):
        args = given_command.split()

        expected_command = expected_masked_command.split()

        # `variables set` parses the key and value into the namespace; the masker reads
        # them from there rather than guessing their position in argv.
        exec_date = timezone.utcnow()
        namespace = Namespace(
            dag_id="foo",
            task_id="bar",
            subcommand="test",
            execution_date=exec_date,
            key=var_key,
            value=var_value,
        )
        with (
            mock.patch.object(sys, "argv", args),
            mock.patch("airflow.utils.session.create_session") as mock_create_session,
        ):
            metrics = cli._build_metrics(args[1], namespace)
            # Make it so the default_action_log doesn't actually commit the txn, by giving it a next txn
            # instead
            mock_create_session.return_value = session.begin_nested()
            mock_create_session.return_value.bulk_insert_mappings = session.bulk_insert_mappings
            cli_action_loggers.default_action_log(**metrics)

            log = session.scalar(select(Log).order_by(Log.dttm.desc()))

        assert metrics.get("start_datetime") <= timezone.utcnow()

        command: str = json.loads(log.extra).get("full_command")
        # Replace single quotes to double quotes to avoid json decode error
        command = ast.literal_eval(command)
        assert command == expected_command


@contextmanager
def fail_action_logger_callback():
    """Adding failing callback and revert it back when closed."""
    tmp = cli_action_loggers.__pre_exec_callbacks[:]

    def fail_callback(**_):
        raise NotImplementedError

    cli_action_loggers.register_pre_exec_callback(fail_callback)
    yield
    cli_action_loggers.__pre_exec_callbacks = tmp


@cli.action_cli(check_db=False)
def fail_func(_):
    raise NotImplementedError


@cli.action_cli(check_db=False)
def success_func(_):
    pass


@cli.action_cli(check_db=False)
def returns_ran(_):
    return "ran"


def test__search_for_dags_file():
    dags_folder = settings.DAGS_FOLDER
    assert _search_for_dag_file("") is None
    assert _search_for_dag_file(None) is None
    # if it's a file, and one can be find in subdir, should return full path
    assert _search_for_dag_file("any/hi/test_example_bash_operator.py") == str(
        Path(dags_folder) / "test_example_bash_operator.py"
    )
    # if a folder, even if exists, should return dags folder
    existing_folder = Path(settings.DAGS_FOLDER, "subdir1")
    assert existing_folder.exists()
    assert _search_for_dag_file(existing_folder.as_posix()) is None
    # when multiple files found, default to the dags folder
    assert _search_for_dag_file("any/hi/__init__.py") is None


def _mock_bundle(path: Path) -> mock.MagicMock:
    bundle = mock.MagicMock(spec=BaseDagBundle, path=path, version=None)
    bundle.name = "testing"
    return bundle


@pytest.mark.parametrize("bundle_names", [["testing"], None], ids=["named-bundle", "every-bundle"])
@mock.patch("airflow.dag_processing.dagbag.sync_bag_to_db", autospec=True)
@mock.patch("airflow.dag_processing.dagbag.BundleDagBag", autospec=True)
@mock.patch.object(cli, "DagBundlesManager", autospec=True)
def test_get_bagged_dag_refuses_a_native_dag(mock_manager, mock_bag, mock_sync, bundle_names):
    mock_manager.return_value.get_all_dag_bundles.return_value = [_mock_bundle(Path("/bundle"))]
    native = DagSerialization.from_dict(DagSerialization.to_dict(DAG("native", schedule=None)))
    mock_bag.return_value.dags = {"native": native}

    with pytest.raises(SystemExit, match="is a native Lang-SDK Dag"):
        cli.get_bagged_dag(bundle_names, "native")

    assert mock_bag.call_args.kwargs["parse_lang_sdk_files"] is False


@pytest.mark.parametrize("bundle_names", [["testing"], None], ids=["named-bundle", "every-bundle"])
@mock.patch("airflow.dag_processing.dagbag.sync_bag_to_db", autospec=True)
@mock.patch("airflow.dag_processing.dagbag.BundleDagBag", autospec=True)
@mock.patch.object(cli, "DagBundlesManager", autospec=True)
def test_get_bagged_dag_returns_a_native_dag_when_allowed(mock_manager, mock_bag, mock_sync, bundle_names):
    mock_manager.return_value.get_all_dag_bundles.return_value = [_mock_bundle(Path("/bundle"))]
    native = DagSerialization.from_dict(DagSerialization.to_dict(DAG("native", schedule=None)))
    mock_bag.return_value.dags = {"native": native}

    assert cli.get_bagged_dag(bundle_names, "native", allow_lang_sdk_dag=True) is native
    assert mock_bag.call_args.kwargs["parse_lang_sdk_files"] is True


@pytest.fixture
def _clear_db_dags():
    db.clear_db_dags()
    db.clear_db_serialized_dags()
    yield
    db.clear_db_dags()
    db.clear_db_serialized_dags()


@pytest.mark.parametrize("allow_lang_sdk_dag", [False, True])
@pytest.mark.usefixtures("_clear_db_dags", "testing_dag_bundle")
@mock.patch.object(LangSDKDagFileProcessorProcess, "run", autospec=True)
@mock.patch.object(cli, "DagBundlesManager", autospec=True)
def test_get_bagged_dag_returns_a_python_dag_bagged_with_a_native_one(
    mock_manager, mock_run, tmp_path, allow_lang_sdk_dag
):
    (tmp_path / "python_dag.py").write_text(
        "from airflow.sdk import DAG\nwith DAG('python_dag', schedule=None): pass\n"
    )
    native_file = write_native_file(tmp_path / "dags.native")
    with DAG("native_dag", schedule=None) as native_dag:
        BaseOperator(task_id="extract")
    mock_run.return_value = DagFileParsingResult(
        fileloc=os.fspath(native_file), serialized_dags=[LazyDeserializedDAG.from_dag(native_dag)]
    )
    mock_manager.return_value.get_all_dag_bundles.return_value = [_mock_bundle(tmp_path)]

    with fake_coordinator():
        dag = cli.get_bagged_dag(None, "python_dag", allow_lang_sdk_dag=allow_lang_sdk_dag)

    assert isinstance(dag, DAG)
    assert dag.dag_id == "python_dag"
    assert mock_run.call_count == int(allow_lang_sdk_dag)
    with create_session() as session:
        assert set(session.scalars(select(DagModel.dag_id))) == {"python_dag"}


def test_validate_dag_bundle_arg():
    with pytest.raises(SystemExit, match="Bundles not found: (x, y)|(y, x)"):
        cli.validate_dag_bundle_arg(["x", "y", "dags-folder"])

    # doesn't raise
    cli.validate_dag_bundle_arg(["dags-folder"])


@pytest.mark.parametrize(
    ("dev_flag", "env_var", "expected"),
    [
        # --dev flag tests
        (True, None, True),
        (False, None, False),
        (None, None, False),  # no dev flag attribute
        # DEV_MODE env var tests
        (False, "true", True),
        (False, "false", False),
        (False, "TRUE", True),
        (False, "True", True),
        # --dev flag takes precedence
        (True, "false", True),
        # Invalid env var values
        (False, "yes", False),
        (False, "1", False),
    ],
)
def test_should_enable_hot_reload(dev_flag, env_var, expected):
    """Test should_enable_hot_reload with various --dev flag and DEV_MODE env var combinations."""
    args = Namespace() if dev_flag is None else Namespace(dev=dev_flag)
    env = {} if env_var is None else {"DEV_MODE": env_var}

    with mock.patch.dict(os.environ, env, clear=True):
        assert cli.should_enable_hot_reload(args) is expected
