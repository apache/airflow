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

import os
from unittest import mock

import pytest
from sqlalchemy.orm import make_transient

from airflow.models.renderedtifields import (
    RenderedTaskInstanceFields,
    RenderedTaskInstanceFields as RTIF,
)
from airflow.providers.cncf.kubernetes.template_rendering import (
    get_rendered_k8s_spec,
    render_k8s_pod_yaml,
)
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.version import version

from tests_common.test_utils.compat import BashOperator

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]

DEFAULT_DATE = timezone.datetime(2021, 9, 9)


@mock.patch.dict(os.environ, {"AIRFLOW_IS_K8S_EXECUTOR_POD": "True"})
@mock.patch("airflow.settings.pod_mutation_hook")
def test_render_k8s_pod_yaml(pod_mutation_hook, create_task_instance):
    ti = create_task_instance(
        dag_id="test_render_k8s_pod_yaml",
        run_id="test_run_id",
        task_id="op1",
        execution_date=DEFAULT_DATE,
    )

    expected_pod_spec = {
        "metadata": {
            "annotations": {
                "dag_id": "test_render_k8s_pod_yaml",
                "run_id": "test_run_id",
                "task_id": "op1",
                "try_number": mock.ANY,
            },
            "labels": {
                "airflow-worker": "0",
                "airflow_version": version,
                "dag_id": "test_render_k8s_pod_yaml",
                "run_id": "test_run_id",
                "kubernetes_executor": "True",
                "task_id": "op1",
                "try_number": mock.ANY,
            },
            "name": mock.ANY,
            "namespace": "default",
        },
        "spec": {
            "containers": [
                {
                    "args": [
                        "airflow",
                        "tasks",
                        "run",
                        "test_render_k8s_pod_yaml",
                        "op1",
                        "test_run_id",
                        "--subdir",
                        __file__,
                    ],
                    "name": "base",
                    "env": [{"name": "AIRFLOW_IS_K8S_EXECUTOR_POD", "value": "True"}],
                }
            ]
        },
    }

    assert render_k8s_pod_yaml(ti) == expected_pod_spec
    pod_mutation_hook.assert_called_once_with(mock.ANY)


@mock.patch.dict(os.environ, {"AIRFLOW_IS_K8S_EXECUTOR_POD": "True"})
@mock.patch.object(RenderedTaskInstanceFields, "get_k8s_pod_yaml")
@mock.patch("airflow.providers.cncf.kubernetes.template_rendering.render_k8s_pod_yaml")
def test_get_rendered_k8s_spec(
    render_k8s_pod_yaml, rtif_get_k8s_pod_yaml, create_task_instance
):
    # Create new TI for the same Task
    ti = create_task_instance()

    mock.patch.object(ti, "render_k8s_pod_yaml", autospec=True)

    fake_spec = {"ermagawds": "pods"}

    session = mock.Mock()

    rtif_get_k8s_pod_yaml.return_value = fake_spec
    assert get_rendered_k8s_spec(ti, session=session) == fake_spec

    rtif_get_k8s_pod_yaml.assert_called_once_with(ti, session=session)
    render_k8s_pod_yaml.assert_not_called()

    # Now test that when we _dont_ find it in the DB, it calls render_k8s_pod_yaml
    rtif_get_k8s_pod_yaml.return_value = None
    render_k8s_pod_yaml.return_value = fake_spec

    assert get_rendered_k8s_spec(session) == fake_spec

    render_k8s_pod_yaml.assert_called_once()


@mock.patch.dict(os.environ, {"AIRFLOW_IS_K8S_EXECUTOR_POD": "True"})
@mock.patch(
    "airflow.utils.log.secrets_masker.redact",
    autospec=True,
    side_effect=lambda d, _=None: d,
)
@mock.patch("airflow.providers.cncf.kubernetes.template_rendering.render_k8s_pod_yaml")
def test_get_k8s_pod_yaml(render_k8s_pod_yaml, redact, dag_maker, session):
    """
    Test that k8s_pod_yaml is rendered correctly, stored in the Database,
    and are correctly fetched using RTIF.get_k8s_pod_yaml
    """
    with dag_maker("test_get_k8s_pod_yaml") as dag:
        task = BashOperator(task_id="test", bash_command="echo hi")
    dr = dag_maker.create_dagrun()
    dag.fileloc = "/test_get_k8s_pod_yaml.py"

    ti = dr.task_instances[0]
    ti.task = task

    render_k8s_pod_yaml.return_value = {"I'm a": "pod"}

    rtif = RTIF(ti=ti)

    assert ti.dag_id == rtif.dag_id
    assert ti.task_id == rtif.task_id
    assert ti.run_id == rtif.run_id

    expected_pod_yaml = {"I'm a": "pod"}

    assert rtif.k8s_pod_yaml == render_k8s_pod_yaml.return_value
    # K8s pod spec dict was passed to redact
    redact.assert_any_call(rtif.k8s_pod_yaml)

    with create_session() as session:
        session.add(rtif)
        session.flush()

        assert expected_pod_yaml == RTIF.get_k8s_pod_yaml(ti=ti, session=session)
        make_transient(ti)
        # "Delete" it from the DB
        session.rollback()

        # Test the else part of get_k8s_pod_yaml
        # i.e. for the TIs that are not stored in RTIF table
        # Fetching them will return None
        assert RTIF.get_k8s_pod_yaml(ti=ti, session=session) is None
