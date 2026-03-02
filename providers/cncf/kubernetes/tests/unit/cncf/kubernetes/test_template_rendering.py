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
import yaml
from kubernetes.client import models as k8s
from sqlalchemy.orm import make_transient

from airflow.models.renderedtifields import RenderedTaskInstanceFields, RenderedTaskInstanceFields as RTIF
from airflow.providers.cncf.kubernetes.template_rendering import get_rendered_k8s_spec, render_k8s_pod_yaml
from airflow.utils import timezone  # type: ignore[attr-defined]
from airflow.utils.session import create_session
from airflow.version import version

from tests_common.test_utils.compat import BashOperator
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

pytestmark = pytest.mark.db_test

DEFAULT_DATE = timezone.datetime(2021, 9, 9)


@mock.patch.dict(os.environ, {"AIRFLOW_IS_K8S_EXECUTOR_POD": "True"})
@mock.patch("airflow.settings.pod_mutation_hook")
def test_render_k8s_pod_yaml(pod_mutation_hook, create_task_instance):
    ti = create_task_instance(
        dag_id="test_render_k8s_pod_yaml",
        run_id="test_run_id",
        task_id="op1",
        logical_date=DEFAULT_DATE,
    )

    if AIRFLOW_V_3_0_PLUS:
        from airflow.executors import workloads

        workload = workloads.ExecuteTask.make(ti)
        rendered_args = [
            "python",
            "-m",
            "airflow.sdk.execution_time.execute_workload",
            "--json-string",
            workload.model_dump_json(),
        ]
    else:
        rendered_args = [
            "airflow",
            "tasks",
            "run",
            "test_render_k8s_pod_yaml",
            "op1",
            "test_run_id",
            "--subdir",
            mock.ANY,
        ]

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
                    "args": rendered_args,
                    "name": "base",
                    "env": [{"name": "AIRFLOW_IS_K8S_EXECUTOR_POD", "value": "True"}],
                }
            ]
        },
    }
    assert render_k8s_pod_yaml(ti) == expected_pod_spec
    pod_mutation_hook.assert_called_once_with(mock.ANY)


@mock.patch.dict(os.environ, {"AIRFLOW_IS_K8S_EXECUTOR_POD": "True"})
@mock.patch("airflow.settings.pod_mutation_hook")
def test_render_k8s_pod_yaml_with_custom_pod_template(pod_mutation_hook, create_task_instance, tmp_path):
    with open(f"{tmp_path}/custom_pod_template.yaml", "w") as ptf:
        template = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"labels": {"custom_label": "custom_value"}},
        }
        ptf.write(yaml.dump(template))

    ti = create_task_instance(
        dag_id="test_render_k8s_pod_yaml",
        run_id="test_run_id",
        task_id="op1",
        logical_date=DEFAULT_DATE,
        executor_config={"pod_template_file": f"{tmp_path}/custom_pod_template.yaml"},
    )

    ti_pod_yaml = render_k8s_pod_yaml(ti)
    assert "custom_label" in ti_pod_yaml["metadata"]["labels"]
    assert ti_pod_yaml["metadata"]["labels"]["custom_label"] == "custom_value"


@mock.patch.dict(os.environ, {"AIRFLOW_IS_K8S_EXECUTOR_POD": "True"})
@mock.patch("airflow.settings.pod_mutation_hook")
def test_render_k8s_pod_yaml_with_custom_pod_template_and_pod_override(
    pod_mutation_hook, create_task_instance, tmp_path
):
    with open(f"{tmp_path}/custom_pod_template.yaml", "w") as ptf:
        template = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"labels": {"custom_label": "custom_value"}},
        }
        ptf.write(yaml.dump(template))

    pod_override = k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(annotations={"test": "annotation"}, labels={"custom_label": "override"})
    )
    ti = create_task_instance(
        dag_id="test_render_k8s_pod_yaml",
        run_id="test_run_id",
        task_id="op1",
        logical_date=DEFAULT_DATE,
        executor_config={
            "pod_template_file": f"{tmp_path}/custom_pod_template.yaml",
            "pod_override": pod_override,
        },
    )

    ti_pod_yaml = render_k8s_pod_yaml(ti)
    assert "custom_label" in ti_pod_yaml["metadata"]["labels"]
    # The initial value associated with the custom_label label in the pod_template_file
    # was overridden by the pod_override
    assert ti_pod_yaml["metadata"]["labels"]["custom_label"] == "override"
    assert ti_pod_yaml["metadata"]["annotations"]["test"] == "annotation"


@pytest.mark.skipif(
    AIRFLOW_V_3_0_PLUS,
    reason="This test is only needed for Airflow 2 - we can remove it after "
    "only Airflow 3 is supported in providers",
)
@mock.patch.dict(os.environ, {"AIRFLOW_IS_K8S_EXECUTOR_POD": "True"})
@mock.patch.object(RenderedTaskInstanceFields, "get_k8s_pod_yaml")
@mock.patch("airflow.providers.cncf.kubernetes.template_rendering.render_k8s_pod_yaml")
def test_get_rendered_k8s_spec(render_k8s_pod_yaml, rtif_get_k8s_pod_yaml, create_task_instance):
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


@pytest.mark.enable_redact
@mock.patch.dict(os.environ, {"AIRFLOW_IS_K8S_EXECUTOR_POD": "True"})
@mock.patch("airflow.providers.cncf.kubernetes.template_rendering.render_k8s_pod_yaml")
def test_get_k8s_pod_yaml(render_k8s_pod_yaml, dag_maker, session):
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

    render_k8s_pod_yaml.return_value = {"I'm a": "pod", "secret": "password123"}

    rtif = RTIF(ti=ti, render_templates=False)

    assert ti.dag_id == rtif.dag_id
    assert ti.task_id == rtif.task_id
    assert ti.run_id == rtif.run_id

    # Expect redacted version
    expected_pod_yaml = {"I'm a": "pod", "secret": "***"}

    assert rtif.k8s_pod_yaml == expected_pod_yaml

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
