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

import random
import re
import string
from unittest import mock
from unittest.mock import patch

import pendulum
import pytest
from kubernetes.client import ApiClient, models as k8s

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.models import DAG, DagModel, DagRun, TaskInstance
from airflow.providers.cncf.kubernetes.operators.job import (
    KubernetesDeleteJobOperator,
    KubernetesJobOperator,
    KubernetesPatchJobOperator,
)
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.types import DagRunType

from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

DEFAULT_DATE = timezone.datetime(2016, 1, 1, 1, 0, 0)
JOB_OPERATORS_PATH = "airflow.providers.cncf.kubernetes.operators.job.{}"
HOOK_CLASS = JOB_OPERATORS_PATH.format("KubernetesHook")
POLL_INTERVAL = 100
JOB_NAME = "test-job"
JOB_NAMESPACE = "test-namespace"
JOB_POLL_INTERVAL = 20.0
KUBERNETES_CONN_ID = "test-conn_id"
POD_NAME = "test-pod"
POD_NAMESPACE = "test-namespace"
TEST_XCOM_RESULT = '{"result": "test-xcom-result"}'
POD_MANAGER_CLASS = "airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager"
ON_KILL_PROPAGATION_POLICY = "Foreground"


def create_context(task, persist_to_db=False, map_index=None):
    if task.has_dag():
        dag = task.dag
    else:
        dag = DAG(dag_id="dag", schedule=None, start_date=pendulum.now())
        dag.add_task(task)
    if AIRFLOW_V_3_0_PLUS:
        sync_dag_to_db(dag)
        dag_run = DagRun(
            run_id=DagRun.generate_run_id(
                run_type=DagRunType.MANUAL, logical_date=DEFAULT_DATE, run_after=DEFAULT_DATE
            ),
            run_type=DagRunType.MANUAL,
            dag_id=dag.dag_id,
        )
    else:
        dag_run = DagRun(
            run_id=DagRun.generate_run_id(DagRunType.MANUAL, DEFAULT_DATE),
            run_type=DagRunType.MANUAL,
            dag_id=dag.dag_id,
        )
    if AIRFLOW_V_3_0_PLUS:
        from airflow.models.dag_version import DagVersion

        dag_version = DagVersion.get_latest_version(dag.dag_id)
        task_instance = TaskInstance(task=task, run_id=dag_run.run_id, dag_version_id=dag_version.id)
    else:
        task_instance = TaskInstance(task=task, run_id=dag_run.run_id)
    task_instance.dag_run = dag_run
    if map_index is not None:
        task_instance.map_index = map_index
    if persist_to_db:
        with create_session() as session:
            if not AIRFLOW_V_3_0_PLUS:
                session.add(DagModel(dag_id=dag.dag_id))
            session.add(dag_run)
            session.add(task_instance)
            session.commit()
    return {
        "dag": dag,
        "ts": DEFAULT_DATE.isoformat(),
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
        "run_id": "test",
    }


@pytest.mark.db_test
@pytest.mark.execution_timeout(300)
class TestKubernetesJobOperator:
    @pytest.fixture(autouse=True)
    def setup_tests(self):
        self._default_client_patch = patch(f"{HOOK_CLASS}._get_default_client")
        self._default_client_mock = self._default_client_patch.start()

        yield

        patch.stopall()

    def test_templates(self, create_task_instance_of_operator, session):
        dag_id = "TestKubernetesJobOperator"
        ti = create_task_instance_of_operator(
            KubernetesJobOperator,
            dag_id=dag_id,
            task_id="task-id",
            namespace="{{ dag.dag_id }}",
            container_resources=k8s.V1ResourceRequirements(
                requests={"memory": "{{ dag.dag_id }}", "cpu": "{{ dag.dag_id }}"},
                limits={"memory": "{{ dag.dag_id }}", "cpu": "{{ dag.dag_id }}"},
            ),
            volume_mounts=[
                k8s.V1VolumeMount(
                    name="{{ dag.dag_id }}",
                    mount_path="mount_path",
                    sub_path="{{ dag.dag_id }}",
                )
            ],
            job_template_file="{{ dag.dag_id }}",
            config_file="{{ dag.dag_id }}",
            labels="{{ dag.dag_id }}",
            env_vars=["{{ dag.dag_id }}"],
            arguments="{{ dag.dag_id }}",
            cmds="{{ dag.dag_id }}",
            image="{{ dag.dag_id }}",
            annotations={"dag-id": "{{ dag.dag_id }}"},
            session=session,
        )

        session.add(ti)
        session.commit()

        rendered = ti.render_templates()

        assert dag_id == rendered.container_resources.limits["memory"]
        assert dag_id == rendered.container_resources.limits["cpu"]
        assert dag_id == rendered.container_resources.requests["memory"]
        assert dag_id == rendered.container_resources.requests["cpu"]
        assert dag_id == rendered.volume_mounts[0].name
        assert dag_id == rendered.volume_mounts[0].sub_path
        assert dag_id == ti.task.image
        assert dag_id == ti.task.cmds
        assert dag_id == ti.task.namespace
        assert dag_id == ti.task.config_file
        assert dag_id == ti.task.labels
        assert dag_id == ti.task.job_template_file
        assert dag_id == ti.task.arguments
        assert dag_id == ti.task.env_vars[0]
        assert dag_id == rendered.annotations["dag-id"]

    def sanitize_for_serialization(self, obj):
        return ApiClient().sanitize_for_serialization(obj)

    def test_backoff_limit_correctly_set(self, clean_dags_dagruns_and_dagbundles):
        k = KubernetesJobOperator(
            task_id="task",
            backoff_limit=6,
        )
        job = k.build_job_request_obj(create_context(k))
        assert job.spec.backoff_limit == 6

    def test_completion_mode_correctly_set(self, clean_dags_dagruns_and_dagbundles):
        k = KubernetesJobOperator(
            task_id="task",
            completion_mode="NonIndexed",
        )
        job = k.build_job_request_obj(create_context(k))
        assert job.spec.completion_mode == "NonIndexed"

    def test_completions_correctly_set(self, clean_dags_dagruns_and_dagbundles):
        k = KubernetesJobOperator(
            task_id="task",
            completions=1,
        )
        job = k.build_job_request_obj(create_context(k))
        assert job.spec.completions == 1

    def test_manual_selector_correctly_set(self, clean_dags_dagruns_and_dagbundles):
        k = KubernetesJobOperator(
            task_id="task",
            manual_selector=False,
        )
        job = k.build_job_request_obj(create_context(k))
        assert job.spec.manual_selector is False

    def test_parallelism_correctly_set(self, clean_dags_dagruns_and_dagbundles):
        k = KubernetesJobOperator(
            task_id="task",
            parallelism=2,
        )
        job = k.build_job_request_obj(create_context(k))
        assert job.spec.parallelism == 2

    def test_selector(self, clean_dags_dagruns_and_dagbundles):
        selector = k8s.V1LabelSelector(
            match_expressions=[],
            match_labels={"foo": "bar", "hello": "airflow"},
        )

        k = KubernetesJobOperator(
            task_id="task",
            selector=selector,
        )

        job = k.build_job_request_obj(create_context(k))
        assert isinstance(job.spec.selector, k8s.V1LabelSelector)
        assert job.spec.selector == selector

    def test_suspend_correctly_set(self, clean_dags_dagruns_and_dagbundles):
        k = KubernetesJobOperator(
            task_id="task",
            suspend=True,
        )
        job = k.build_job_request_obj(create_context(k))
        assert job.spec.suspend is True

    def test_ttl_seconds_after_finished_correctly_set(self, clean_dags_dagruns_and_dagbundles):
        k = KubernetesJobOperator(task_id="task", ttl_seconds_after_finished=5)
        job = k.build_job_request_obj(create_context(k))
        assert job.spec.ttl_seconds_after_finished == 5

    @pytest.mark.parametrize("randomize", [True, False])
    def test_provided_job_name(self, randomize, clean_dags_dagruns_and_dagbundles):
        name_base = "test"
        k = KubernetesJobOperator(
            name=name_base,
            random_name_suffix=randomize,
            task_id="task",
        )
        context = create_context(k)
        job = k.build_job_request_obj(context)

        if randomize:
            assert job.metadata.name.startswith(f"job-{name_base}")
            assert job.metadata.name != f"job-{name_base}"
        else:
            assert job.metadata.name == f"job-{name_base}"

    @pytest.fixture
    def job_spec(self):
        return k8s.V1Job(
            metadata=k8s.V1ObjectMeta(name="hello", labels={"foo": "bar"}, namespace="jobspecnamespace"),
            spec=k8s.V1JobSpec(
                template=k8s.V1PodTemplateSpec(
                    metadata=k8s.V1ObjectMeta(
                        name="world", labels={"foo": "bar"}, namespace="podspecnamespace"
                    ),
                    spec=k8s.V1PodSpec(
                        containers=[
                            k8s.V1Container(
                                name="base",
                                image="ubuntu:16.04",
                                command=["something"],
                            )
                        ]
                    ),
                )
            ),
        )

    @pytest.mark.parametrize("randomize_name", (True, False))
    def test_full_job_spec(self, randomize_name, job_spec, clean_dags_dagruns_and_dagbundles):
        job_spec_name_base = job_spec.metadata.name

        k = KubernetesJobOperator(
            task_id="task",
            random_name_suffix=randomize_name,
            full_job_spec=job_spec,
        )
        context = create_context(k)
        job = k.build_job_request_obj(context)

        if randomize_name:
            assert job.metadata.name.startswith(f"job-{job_spec_name_base}")
            assert job.metadata.name != f"job-{job_spec_name_base}"
        else:
            assert job.metadata.name == f"job-{job_spec_name_base}"
        assert job.metadata.namespace == job_spec.metadata.namespace
        assert job.spec.template.spec.containers[0].image == job_spec.spec.template.spec.containers[0].image
        assert (
            job.spec.template.spec.containers[0].command == job_spec.spec.template.spec.containers[0].command
        )
        assert job.metadata.labels == {"foo": "bar"}

    @pytest.mark.parametrize("randomize_name", (True, False))
    def test_full_job_spec_kwargs(self, randomize_name, job_spec, clean_dags_dagruns_and_dagbundles):
        # kwargs take precedence, however
        image = "some.custom.image:andtag"
        name_base = "world"
        k = KubernetesJobOperator(
            task_id="task",
            random_name_suffix=randomize_name,
            full_job_spec=job_spec,
            name=name_base,
            image=image,
            labels={"hello": "world"},
        )
        job = k.build_job_request_obj(create_context(k))

        # make sure the kwargs takes precedence (and that name is randomized when expected)
        if randomize_name:
            assert job.metadata.name.startswith(f"job-{name_base}")
            assert job.metadata.name != f"job-{name_base}"
        else:
            assert job.metadata.name == f"job-{name_base}"
        assert job.spec.template.spec.containers[0].image == image
        assert job.metadata.labels == {
            "foo": "bar",
            "hello": "world",
        }

    @pytest.fixture
    def job_template_file(self, tmp_path):
        job_template_yaml = """
            apiVersion: batch/v1
            kind: Job
            metadata:
              name: hello
              namespace: templatenamespace
              labels:
                foo: bar
            spec:
              ttlSecondsAfterFinished: 60
              parallelism: 3
              completions: 3
              suspend: true
              template:
                spec:
                  serviceAccountName: foo
                  affinity:
                    nodeAffinity:
                      requiredDuringSchedulingIgnoredDuringExecution:
                        nodeSelectorTerms:
                          - matchExpressions:
                            - key: kubernetes.io/role
                              operator: In
                              values:
                                - foo
                                - bar
                      preferredDuringSchedulingIgnoredDuringExecution:
                        - weight: 1
                          preference:
                            matchExpressions:
                              - key: kubernetes.io/role
                                operator: In
                                values:
                                  - foo
                                  - bar
                  containers:
                    - name: base
                      image: ubuntu:16.04
                      imagePullPolicy: Always
                      command:
                        - something
        """

        tpl_file = tmp_path / "template.yaml"
        tpl_file.write_text(job_template_yaml)

        return tpl_file

    @pytest.mark.parametrize("randomize_name", (True, False))
    def test_job_template_file(self, randomize_name, job_template_file, clean_dags_dagruns_and_dagbundles):
        k = KubernetesJobOperator(
            task_id="task",
            random_name_suffix=randomize_name,
            job_template_file=job_template_file,
        )
        job = k.build_job_request_obj(create_context(k))

        if randomize_name:
            assert job.metadata.name.startswith("job-hello")
            assert job.metadata.name != "job-hello"
        else:
            assert job.metadata.name == "job-hello"
        assert job.metadata.labels == {"foo": "bar"}
        assert job.metadata.namespace == "templatenamespace"
        assert job.spec.template.spec.containers[0].image == "ubuntu:16.04"
        assert job.spec.template.spec.containers[0].image_pull_policy == "Always"
        assert job.spec.template.spec.containers[0].command == ["something"]
        assert job.spec.template.spec.service_account_name == "foo"
        affinity = {
            "node_affinity": {
                "preferred_during_scheduling_ignored_during_execution": [
                    {
                        "preference": {
                            "match_expressions": [
                                {"key": "kubernetes.io/role", "operator": "In", "values": ["foo", "bar"]}
                            ],
                            "match_fields": None,
                        },
                        "weight": 1,
                    }
                ],
                "required_during_scheduling_ignored_during_execution": {
                    "node_selector_terms": [
                        {
                            "match_expressions": [
                                {"key": "kubernetes.io/role", "operator": "In", "values": ["foo", "bar"]}
                            ],
                            "match_fields": None,
                        }
                    ]
                },
            },
            "pod_affinity": None,
            "pod_anti_affinity": None,
        }

        assert job.spec.template.spec.affinity.to_dict() == affinity

    @pytest.mark.parametrize("randomize_name", (True, False))
    def test_job_template_file_kwargs_override(
        self, randomize_name, job_template_file, clean_dags_dagruns_and_dagbundles
    ):
        # kwargs take precedence, however
        image = "some.custom.image:andtag"
        name_base = "world"
        k = KubernetesJobOperator(
            task_id="task",
            job_template_file=job_template_file,
            name=name_base,
            random_name_suffix=randomize_name,
            image=image,
            labels={"hello": "world"},
        )
        job = k.build_job_request_obj(create_context(k))

        # make sure the kwargs takes precedence (and that name is randomized when expected)
        if randomize_name:
            assert job.metadata.name.startswith(f"job-{name_base}")
            assert job.metadata.name != f"job-{name_base}"
        else:
            assert job.metadata.name == f"job-{name_base}"
        assert job.spec.template.spec.containers[0].image == image
        assert job.metadata.labels == {
            "foo": "bar",
            "hello": "world",
        }

    def test_task_id_as_name(self):
        k = KubernetesJobOperator(
            task_id=".hi.-_09HI",
            random_name_suffix=False,
        )
        job = k.build_job_request_obj({})
        assert job.metadata.name == "job-hi-09hi"

    def test_task_id_as_name_with_suffix(self):
        k = KubernetesJobOperator(
            task_id=".hi.-_09HI",
            random_name_suffix=True,
        )
        job = k.build_job_request_obj({})
        expected = "job-hi-09hi"
        assert job.metadata.name[: len(expected)] == expected
        assert re.match(rf"{expected}-[a-z0-9]{{8}}", job.metadata.name) is not None

    def test_task_id_as_name_with_suffix_very_long(self):
        k = KubernetesJobOperator(
            task_id="a" * 250,
            random_name_suffix=True,
        )
        job = k.build_job_request_obj({})
        assert (
            re.match(
                r"job-a{50}-[a-z0-9]{8}",
                job.metadata.name,
            )
            is not None
        )

    def test_task_id_as_name_dag_id_is_ignored(self):
        dag = DAG(dag_id="this_is_a_dag_name", schedule=None, start_date=pendulum.now())
        k = KubernetesJobOperator(
            task_id="a_very_reasonable_task_name",
            dag=dag,
        )
        job = k.build_job_request_obj({})
        assert re.match(r"job-a-very-reasonable-task-name-[a-z0-9-]+", job.metadata.name) is not None

    @pytest.mark.non_db_test_override
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator.get_or_create_pod"))
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator.build_job_request_obj"))
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator.create_job"))
    @patch(HOOK_CLASS)
    def test_execute(self, mock_hook, mock_create_job, mock_build_job_request_obj, mock_get_or_create_pod):
        mock_hook.return_value.is_job_failed.return_value = False
        mock_job_request_obj = mock_build_job_request_obj.return_value
        mock_job_expected = mock_create_job.return_value
        mock_ti = mock.MagicMock()
        context = dict(ti=mock_ti)

        op = KubernetesJobOperator(
            task_id="test_task_id",
        )
        with pytest.warns(AirflowProviderDeprecationWarning):
            execute_result = op.execute(context=context)

        mock_build_job_request_obj.assert_called_once_with(context)
        mock_create_job.assert_called_once_with(job_request_obj=mock_job_request_obj)
        mock_ti.xcom_push.assert_has_calls(
            [
                mock.call(key="job_name", value=mock_job_expected.metadata.name),
                mock.call(key="job_namespace", value=mock_job_expected.metadata.namespace),
                mock.call(key="job", value=mock_job_expected.to_dict.return_value),
            ]
        )

        assert op.job_request_obj == mock_job_request_obj
        assert op.job == mock_job_expected
        assert not op.wait_until_job_complete
        assert execute_result is None
        assert not mock_hook.wait_until_job_complete.called

    @pytest.mark.non_db_test_override
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator.get_pods"))
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator.build_job_request_obj"))
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator.create_job"))
    @patch(HOOK_CLASS)
    def test_execute_with_parallelism(
        self, mock_hook, mock_create_job, mock_build_job_request_obj, mock_get_pods
    ):
        mock_hook.return_value.is_job_failed.return_value = False
        mock_job_request_obj = mock_build_job_request_obj.return_value
        mock_job_expected = mock_create_job.return_value
        mock_get_pods.return_value = [mock.MagicMock(), mock.MagicMock()]
        mock_pods_expected = mock_get_pods.return_value
        mock_ti = mock.MagicMock()
        context = dict(ti=mock_ti)

        op = KubernetesJobOperator(
            task_id="test_task_id",
            parallelism=2,
        )
        execute_result = op.execute(context=context)

        mock_build_job_request_obj.assert_called_once_with(context)
        mock_create_job.assert_called_once_with(job_request_obj=mock_job_request_obj)
        mock_ti.xcom_push.assert_has_calls(
            [
                mock.call(key="job_name", value=mock_job_expected.metadata.name),
                mock.call(key="job_namespace", value=mock_job_expected.metadata.namespace),
                mock.call(key="job", value=mock_job_expected.to_dict.return_value),
            ]
        )

        assert op.job_request_obj == mock_job_request_obj
        assert op.job == mock_job_expected
        assert op.pods == mock_pods_expected
        with pytest.warns(AirflowProviderDeprecationWarning):
            assert op.pod is mock_pods_expected[0]
        assert not op.wait_until_job_complete
        assert execute_result is None
        assert not mock_hook.wait_until_job_complete.called

    @pytest.mark.non_db_test_override
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator.get_or_create_pod"))
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator.build_job_request_obj"))
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator.create_job"))
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator.execute_deferrable"))
    @patch(HOOK_CLASS)
    def test_execute_in_deferrable(
        self,
        mock_hook,
        mock_execute_deferrable,
        mock_create_job,
        mock_build_job_request_obj,
        mock_get_or_create_pod,
    ):
        mock_hook.return_value.is_job_failed.return_value = False
        mock_job_request_obj = mock_build_job_request_obj.return_value
        mock_job_expected = mock_create_job.return_value
        mock_ti = mock.MagicMock()
        context = dict(ti=mock_ti)

        op = KubernetesJobOperator(
            task_id="test_task_id",
            wait_until_job_complete=True,
            deferrable=True,
        )
        with pytest.warns(AirflowProviderDeprecationWarning):
            actual_result = op.execute(context=context)

        mock_build_job_request_obj.assert_called_once_with(context)
        mock_create_job.assert_called_once_with(job_request_obj=mock_job_request_obj)
        mock_ti.xcom_push.assert_has_calls(
            [
                mock.call(key="job_name", value=mock_job_expected.metadata.name),
                mock.call(key="job_namespace", value=mock_job_expected.metadata.namespace),
            ]
        )
        mock_execute_deferrable.assert_called_once()

        assert op.job_request_obj == mock_job_request_obj
        assert op.job == mock_job_expected
        assert actual_result is None
        assert not mock_hook.wait_until_job_complete.called

    @pytest.mark.non_db_test_override
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator.get_or_create_pod"))
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator.build_job_request_obj"))
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator.create_job"))
    @patch(HOOK_CLASS)
    def test_execute_fail(
        self, mock_hook, mock_create_job, mock_build_job_request_obj, mock_get_or_create_pod
    ):
        mock_hook.return_value.is_job_failed.return_value = "Error"

        op = KubernetesJobOperator(
            task_id="test_task_id",
            wait_until_job_complete=True,
        )

        with pytest.warns(AirflowProviderDeprecationWarning):
            with pytest.raises(AirflowException):
                op.execute(context=dict(ti=mock.MagicMock()))

    @pytest.mark.non_db_test_override
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator.defer"))
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobTrigger"))
    def test_execute_deferrable(self, mock_trigger, mock_execute_deferrable):
        mock_cluster_context = mock.MagicMock()
        mock_config_file = mock.MagicMock()
        mock_in_cluster = mock.MagicMock()

        mock_job = mock.MagicMock()
        mock_job.metadata.name = JOB_NAME
        mock_job.metadata.namespace = JOB_NAMESPACE

        mock_pod = mock.MagicMock()
        mock_pod.metadata.name = POD_NAME
        mock_pod.metadata.namespace = POD_NAMESPACE

        mock_trigger_instance = mock_trigger.return_value

        op = KubernetesJobOperator(
            task_id="test_task_id",
            kubernetes_conn_id=KUBERNETES_CONN_ID,
            cluster_context=mock_cluster_context,
            config_file=mock_config_file,
            in_cluster=mock_in_cluster,
            job_poll_interval=POLL_INTERVAL,
            wait_until_job_complete=True,
            deferrable=True,
        )
        op.job = mock_job
        op.pod = mock_pod
        op.pods = [
            mock_pod,
        ]

        actual_result = op.execute_deferrable()

        mock_execute_deferrable.assert_called_once_with(
            trigger=mock_trigger_instance,
            method_name="execute_complete",
        )
        mock_trigger.assert_called_once_with(
            job_name=JOB_NAME,
            job_namespace=JOB_NAMESPACE,
            pod_names=[
                POD_NAME,
            ],
            pod_namespace=POD_NAMESPACE,
            base_container_name=op.BASE_CONTAINER_NAME,
            kubernetes_conn_id=KUBERNETES_CONN_ID,
            cluster_context=mock_cluster_context,
            config_file=mock_config_file,
            in_cluster=mock_in_cluster,
            poll_interval=POLL_INTERVAL,
            get_logs=True,
            do_xcom_push=False,
        )
        assert actual_result is None

    @pytest.mark.non_db_test_override
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator.defer"))
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobTrigger"))
    def test_execute_deferrable_with_parallelism(self, mock_trigger, mock_execute_deferrable):
        mock_cluster_context = mock.MagicMock()
        mock_config_file = mock.MagicMock()
        mock_in_cluster = mock.MagicMock()

        mock_job = mock.MagicMock()
        mock_job.metadata.name = JOB_NAME
        mock_job.metadata.namespace = JOB_NAMESPACE

        pod_name_1 = POD_NAME + "-1"
        mock_pod_1 = mock.MagicMock()
        mock_pod_1.metadata.name = pod_name_1
        mock_pod_1.metadata.namespace = POD_NAMESPACE

        pod_name_2 = POD_NAME + "-2"
        mock_pod_2 = mock.MagicMock()
        mock_pod_2.metadata.name = pod_name_2
        mock_pod_2.metadata.namespace = POD_NAMESPACE

        mock_trigger_instance = mock_trigger.return_value

        op = KubernetesJobOperator(
            task_id="test_task_id",
            kubernetes_conn_id=KUBERNETES_CONN_ID,
            cluster_context=mock_cluster_context,
            config_file=mock_config_file,
            in_cluster=mock_in_cluster,
            job_poll_interval=POLL_INTERVAL,
            parallelism=2,
            wait_until_job_complete=True,
            deferrable=True,
        )
        op.job = mock_job
        op.pods = [mock_pod_1, mock_pod_2]

        actual_result = op.execute_deferrable()

        mock_execute_deferrable.assert_called_once_with(
            trigger=mock_trigger_instance,
            method_name="execute_complete",
        )
        mock_trigger.assert_called_once_with(
            job_name=JOB_NAME,
            job_namespace=JOB_NAMESPACE,
            pod_names=[pod_name_1, pod_name_2],
            pod_namespace=POD_NAMESPACE,
            base_container_name=op.BASE_CONTAINER_NAME,
            kubernetes_conn_id=KUBERNETES_CONN_ID,
            cluster_context=mock_cluster_context,
            config_file=mock_config_file,
            in_cluster=mock_in_cluster,
            poll_interval=POLL_INTERVAL,
            get_logs=True,
            do_xcom_push=False,
        )
        assert actual_result is None

    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator.get_or_create_pod"))
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator.build_job_request_obj"))
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator.create_job"))
    @patch(f"{HOOK_CLASS}.wait_until_job_complete")
    def test_wait_until_job_complete(
        self,
        mock_wait_until_job_complete,
        mock_create_job,
        mock_build_job_request_obj,
        mock_get_or_create_pod,
    ):
        mock_job_expected = mock_create_job.return_value
        mock_ti = mock.MagicMock()

        op = KubernetesJobOperator(
            task_id="test_task_id", wait_until_job_complete=True, job_poll_interval=POLL_INTERVAL
        )
        with pytest.warns(AirflowProviderDeprecationWarning):
            op.execute(context=dict(ti=mock_ti))

        assert op.wait_until_job_complete
        assert op.job_poll_interval == POLL_INTERVAL
        mock_wait_until_job_complete.assert_called_once_with(
            job_name=mock_job_expected.metadata.name,
            namespace=mock_job_expected.metadata.namespace,
            job_poll_interval=POLL_INTERVAL,
        )

    @pytest.mark.parametrize("do_xcom_push", [True, False])
    @pytest.mark.parametrize("get_logs", [True, False])
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator._write_logs"))
    def test_execute_complete(self, mocked_write_logs, get_logs, do_xcom_push):
        mock_ti = mock.MagicMock()
        context = {"ti": mock_ti}
        mock_job = mock.MagicMock()
        event = {
            "job": mock_job,
            "status": "success",
            "pod_names": [
                POD_NAME,
            ]
            if get_logs
            else None,
            "pod_namespace": POD_NAMESPACE if get_logs else None,
            "xcom_result": [
                TEST_XCOM_RESULT,
            ]
            if do_xcom_push
            else None,
        }

        KubernetesJobOperator(
            task_id="test_task_id", get_logs=get_logs, do_xcom_push=do_xcom_push
        ).execute_complete(context=context, event=event)

        mock_ti.xcom_push.assert_called_once_with(key="job", value=mock_job)

        if get_logs:
            mocked_write_logs.assert_called_once()
        else:
            mocked_write_logs.assert_not_called()

    @pytest.mark.non_db_test_override
    def test_execute_complete_fail(self):
        mock_ti = mock.MagicMock()
        context = {"ti": mock_ti}
        mock_job = mock.MagicMock()
        event = {"job": mock_job, "status": "error", "message": "error message"}

        with pytest.raises(AirflowException):
            KubernetesJobOperator(task_id="test_task_id").execute_complete(context=context, event=event)

        mock_ti.xcom_push.assert_called_once_with(key="job", value=mock_job)

    @pytest.mark.non_db_test_override
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator.job_client"))
    def test_on_kill(self, mock_client):
        mock_job = mock.MagicMock()
        mock_job.metadata.name = JOB_NAME
        mock_job.metadata.namespace = JOB_NAMESPACE

        op = KubernetesJobOperator(task_id="test_task_id")
        op.job = mock_job
        op.on_kill()

        mock_client.delete_namespaced_job.assert_called_once_with(
            name=JOB_NAME,
            namespace=JOB_NAMESPACE,
            propagation_policy=ON_KILL_PROPAGATION_POLICY,
        )

    @pytest.mark.non_db_test_override
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator.job_client"))
    def test_on_kill_termination_grace_period(self, mock_client):
        mock_job = mock.MagicMock()
        mock_job.metadata.name = JOB_NAME
        mock_job.metadata.namespace = JOB_NAMESPACE
        mock_termination_grace_period = mock.MagicMock()

        op = KubernetesJobOperator(
            task_id="test_task_id", termination_grace_period=mock_termination_grace_period
        )
        op.job = mock_job
        op.on_kill()

        mock_client.delete_namespaced_job.assert_called_once_with(
            name=JOB_NAME,
            namespace=JOB_NAMESPACE,
            propagation_policy=ON_KILL_PROPAGATION_POLICY,
            grace_period_seconds=mock_termination_grace_period,
        )

    @pytest.mark.non_db_test_override
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator.client"))
    @patch(HOOK_CLASS)
    def test_on_kill_none_job(self, mock_hook, mock_client):
        mock_serialize = mock_hook.return_value.batch_v1_client.api_client.sanitize_for_serialization

        op = KubernetesJobOperator(task_id="test_task_id")
        op.on_kill()

        mock_client.delete_namespaced_job.assert_not_called()
        mock_serialize.assert_not_called()

    @pytest.mark.parametrize("parallelism", [None, 2])
    @pytest.mark.parametrize("do_xcom_push", [True, False])
    @pytest.mark.parametrize("get_logs", [True, False])
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator.extract_xcom"))
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator.get_pods"))
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator.get_or_create_pod"))
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator.build_job_request_obj"))
    @patch(JOB_OPERATORS_PATH.format("KubernetesJobOperator.create_job"))
    @patch(f"{POD_MANAGER_CLASS}.fetch_requested_container_logs")
    @patch(f"{POD_MANAGER_CLASS}.await_xcom_sidecar_container_start")
    @patch(f"{POD_MANAGER_CLASS}.await_container_completion")
    @patch(f"{HOOK_CLASS}.wait_until_job_complete")
    def test_execute_xcom_and_logs(
        self,
        mock_wait_until_job_complete,
        mock_await_container_completion,
        mock_await_xcom_sidecar_container_start,
        mocked_fetch_logs,
        mock_create_job,
        mock_build_job_request_obj,
        mock_get_or_create_pod,
        mock_get_pods,
        mock_extract_xcom,
        get_logs,
        do_xcom_push,
        parallelism,
    ):
        if parallelism == 2:
            mock_pod_1 = mock.MagicMock()
            mock_pod_2 = mock.MagicMock()
            mock_get_pods.return_value = [mock_pod_1, mock_pod_2]
        mock_ti = mock.MagicMock()
        op = KubernetesJobOperator(
            task_id="test_task_id",
            wait_until_job_complete=True,
            job_poll_interval=POLL_INTERVAL,
            get_logs=get_logs,
            do_xcom_push=do_xcom_push,
            parallelism=parallelism,
        )

        if not parallelism:
            with pytest.warns(AirflowProviderDeprecationWarning):
                op.execute(context=dict(ti=mock_ti))
        else:
            op.execute(context=dict(ti=mock_ti))

        if do_xcom_push and not parallelism:
            mock_extract_xcom.assert_called_once()
        elif do_xcom_push and parallelism is not None:
            assert mock_extract_xcom.call_count == parallelism
        else:
            mock_extract_xcom.assert_not_called()

        if get_logs and not parallelism:
            mocked_fetch_logs.assert_called_once()
        elif get_logs and parallelism is not None:
            assert mocked_fetch_logs.call_count == parallelism
        else:
            mocked_fetch_logs.assert_not_called()


@pytest.mark.db_test
@pytest.mark.execution_timeout(300)
class TestKubernetesDeleteJobOperator:
    @pytest.fixture(autouse=True)
    def setup_tests(self):
        self._default_client_patch = patch(f"{HOOK_CLASS}._get_default_client")
        self._default_client_mock = self._default_client_patch.start()

        yield

        patch.stopall()

    @patch(f"{HOOK_CLASS}.get_job_status")
    @patch(f"{HOOK_CLASS}.wait_until_job_complete")
    @patch("kubernetes.config.load_kube_config")
    @patch("kubernetes.client.api.BatchV1Api.delete_namespaced_job")
    def test_execute(
        self,
        mock_delete_namespaced_job,
        mock_load_kube_config,
        mock_wait_until_job_complete,
        mock_get_job_status,
    ):
        op = KubernetesDeleteJobOperator(
            kubernetes_conn_id="kubernetes_default",
            task_id="test_delete_job",
            name=JOB_NAME,
            namespace=JOB_NAMESPACE,
        )

        op.execute(None)

        assert not mock_wait_until_job_complete.called
        mock_get_job_status.assert_called_once_with(job_name=JOB_NAME, namespace=JOB_NAMESPACE)
        mock_delete_namespaced_job.assert_called_once_with(name=JOB_NAME, namespace=JOB_NAMESPACE)

    @patch(f"{HOOK_CLASS}.get_job_status")
    @patch(f"{HOOK_CLASS}.wait_until_job_complete")
    @patch("kubernetes.config.load_kube_config")
    @patch("kubernetes.client.api.BatchV1Api.delete_namespaced_job")
    def test_execute_wait_for_completion_true(
        self,
        mock_delete_namespaced_job,
        mock_load_kube_config,
        mock_wait_until_job_complete,
        mock_get_job_status,
    ):
        op = KubernetesDeleteJobOperator(
            kubernetes_conn_id="kubernetes_default",
            task_id="test_delete_job",
            name=JOB_NAME,
            namespace=JOB_NAMESPACE,
            wait_for_completion=True,
            poll_interval=JOB_POLL_INTERVAL,
        )

        op.execute({})

        mock_wait_until_job_complete.assert_called_once_with(
            job_name=JOB_NAME, namespace=JOB_NAMESPACE, job_poll_interval=JOB_POLL_INTERVAL
        )
        assert not mock_get_job_status.called
        mock_delete_namespaced_job.assert_called_once_with(name=JOB_NAME, namespace=JOB_NAMESPACE)

    @pytest.mark.parametrize(
        ("on_status", "success", "fail", "deleted"),
        [
            (None, True, True, True),
            (None, True, False, True),
            (None, False, True, True),
            (None, False, False, True),
            ("Complete", True, True, True),
            ("Complete", True, False, True),
            ("Complete", False, True, False),
            ("Complete", False, False, False),
            ("Failed", True, True, True),
            ("Failed", True, False, False),
            ("Failed", False, True, True),
            ("Failed", False, False, False),
        ],
    )
    @patch(f"{HOOK_CLASS}.is_job_failed")
    @patch(f"{HOOK_CLASS}.is_job_successful")
    @patch("kubernetes.config.load_kube_config")
    @patch("kubernetes.client.api.BatchV1Api.delete_namespaced_job")
    def test_execute_delete_on_status(
        self,
        mock_delete_namespaced_job,
        mock_load_kube_config,
        mock_is_job_successful,
        mock_is_job_failed,
        on_status,
        success,
        fail,
        deleted,
    ):
        mock_is_job_successful.return_value = success
        mock_is_job_failed.return_value = fail

        op = KubernetesDeleteJobOperator(
            kubernetes_conn_id="kubernetes_default",
            task_id="test_delete_job",
            name=JOB_NAME,
            namespace=JOB_NAMESPACE,
            delete_on_status=on_status,
        )

        op.execute({})

        assert mock_delete_namespaced_job.called == deleted

    def test_execute_delete_on_status_exception(self):
        invalid_delete_on_status = "".join(
            random.choices(string.ascii_letters + string.digits, k=random.randint(1, 16))
        )

        op = KubernetesDeleteJobOperator(
            kubernetes_conn_id="kubernetes_default",
            task_id="test_delete_job",
            name=JOB_NAME,
            namespace=JOB_NAMESPACE,
            delete_on_status=invalid_delete_on_status,
        )

        with pytest.raises(AirflowException):
            op.execute({})


@pytest.mark.execution_timeout(300)
class TestKubernetesPatchJobOperator:
    @pytest.fixture(autouse=True)
    def setup_tests(self):
        self._default_client_patch = patch(f"{HOOK_CLASS}._get_default_client")
        self._default_client_mock = self._default_client_patch.start()

        yield

        patch.stopall()

    @pytest.mark.db_test
    @patch("kubernetes.config.load_kube_config")
    @patch("kubernetes.client.api.BatchV1Api.patch_namespaced_job")
    def test_update_execute(self, mock_patch_namespaced_job, mock_load_kube_config):
        op = KubernetesPatchJobOperator(
            kubernetes_conn_id="kubernetes_default",
            task_id="test_update_job",
            name="test_job_name",
            namespace="test_job_namespace",
            body={"spec": {"suspend": False}},
        )

        op.execute(None)

        mock_patch_namespaced_job.assert_called()
