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

from unittest import mock

import pytest

from tests_common.test_utils.compat import timezone
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG
else:
    from airflow.models.dag import DAG  # type: ignore[no-redef,assignment]

from airflow.providers.amazon.aws.decorators.eks import _EksPodDecoratedOperator, eks_pod_task

DEFAULT_DATE = timezone.datetime(2023, 1, 1)


class TestEksPodDecorator:
    def test_init_builds_eks_pod_operator(self):
        with DAG(dag_id="test_eks_deco_init", schedule=None, start_date=DEFAULT_DATE):

            @eks_pod_task(cluster_name="my-eks", image="python:3.12-slim")
            def f():
                return {"a": 1}

            task = f()

        op = task.operator
        assert isinstance(op, _EksPodDecoratedOperator)
        assert op.custom_operator_name == "@task.eks_pod"
        assert op.task_id == "f"
        assert op.cluster_name == "my-eks"
        assert op.image == "python:3.12-slim"
        assert op.cmds == ["placeholder-command"]
        assert op.random_name_suffix is True
        assert op.pod_name == "eks-airflow-pod-f"

    def test_explicit_pod_name_is_respected(self):
        with DAG(dag_id="test_eks_deco_name", schedule=None, start_date=DEFAULT_DATE):

            @eks_pod_task(cluster_name="my-eks", image="python:3.12-slim", pod_name="custom-pod")
            def f():
                return None

            task = f()

        assert task.operator.pod_name == "custom-pod"

    @mock.patch("airflow.providers.amazon.aws.decorators.eks.EksPodOperator.execute")
    def test_execute_injects_script_then_delegates(self, mock_super_execute):
        with DAG(dag_id="test_eks_deco_exec", schedule=None, start_date=DEFAULT_DATE):

            @eks_pod_task(cluster_name="my-eks", image="python:3.12-slim")
            def f():
                return {"a": 1}

            task = f()

        op = task.operator
        op.execute(context=mock.MagicMock())

        env_names = {env.name for env in op.env_vars}
        assert {"__PYTHON_SCRIPT", "__PYTHON_INPUT"} <= env_names
        assert op.cmds[0] == "bash"
        mock_super_execute.assert_called_once()

    @pytest.mark.parametrize("do_xcom_push", [True, False])
    def test_generate_cmds_handles_xcom(self, do_xcom_push):
        with DAG(dag_id="test_eks_deco_xcom", schedule=None, start_date=DEFAULT_DATE):

            @eks_pod_task(cluster_name="my-eks", image="python:3.12-slim", do_xcom_push=do_xcom_push)
            def f():
                return {"a": 1}

            task = f()

        cmds = task.operator._generate_cmds()
        assert cmds[:2] == ["bash", "-cx"]
        assert ("mkdir -p /airflow/xcom" in cmds[2]) is do_xcom_push
