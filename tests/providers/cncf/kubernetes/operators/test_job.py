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

import re

import pendulum
import pytest
from kubernetes.client import ApiClient, models as k8s

from airflow.models import DAG, DagModel, DagRun, TaskInstance
from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.types import DagRunType

DEFAULT_DATE = timezone.datetime(2016, 1, 1, 1, 0, 0)


def create_context(task, persist_to_db=False, map_index=None):
    if task.has_dag():
        dag = task.dag
    else:
        dag = DAG(dag_id="dag", start_date=pendulum.now())
        dag.add_task(task)
    dag_run = DagRun(
        run_id=DagRun.generate_run_id(DagRunType.MANUAL, DEFAULT_DATE),
        run_type=DagRunType.MANUAL,
        dag_id=dag.dag_id,
    )
    task_instance = TaskInstance(task=task, run_id=dag_run.run_id)
    task_instance.dag_run = dag_run
    if map_index is not None:
        task_instance.map_index = map_index
    if persist_to_db:
        with create_session() as session:
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


@pytest.mark.execution_timeout(300)
class TestKubernetesJobOperator:
    def test_templates(self, create_task_instance_of_operator):
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
        )

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

    def test_backoff_limit_correctly_set(self):
        k = KubernetesJobOperator(
            task_id="task",
            backoff_limit=6,
        )
        job = k.build_job_request_obj(create_context(k))
        assert job.spec.backoff_limit == 6

    def test_completion_mode_correctly_set(self):
        k = KubernetesJobOperator(
            task_id="task",
            completion_mode="NonIndexed",
        )
        job = k.build_job_request_obj(create_context(k))
        assert job.spec.completion_mode == "NonIndexed"

    def test_completions_correctly_set(self):
        k = KubernetesJobOperator(
            task_id="task",
            completions=1,
        )
        job = k.build_job_request_obj(create_context(k))
        assert job.spec.completions == 1

    def test_manual_selector_correctly_set(self):
        k = KubernetesJobOperator(
            task_id="task",
            manual_selector=False,
        )
        job = k.build_job_request_obj(create_context(k))
        assert job.spec.manual_selector is False

    def test_parallelism_correctly_set(self):
        k = KubernetesJobOperator(
            task_id="task",
            parallelism=2,
        )
        job = k.build_job_request_obj(create_context(k))
        assert job.spec.parallelism == 2

    def test_selector(self):
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

    def test_suspend_correctly_set(self):
        k = KubernetesJobOperator(
            task_id="task",
            suspend=True,
        )
        job = k.build_job_request_obj(create_context(k))
        assert job.spec.suspend is True

    def test_ttl_seconds_after_finished_correctly_set(self):
        k = KubernetesJobOperator(task_id="task", ttl_seconds_after_finished=5)
        job = k.build_job_request_obj(create_context(k))
        assert job.spec.ttl_seconds_after_finished == 5

    @pytest.mark.parametrize("randomize", [True, False])
    def test_provided_job_name(self, randomize):
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

    @pytest.mark.parametrize(("randomize_name",), ([True], [False]))
    def test_full_job_spec(self, randomize_name, job_spec):
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

    @pytest.mark.parametrize(("randomize_name",), ([True], [False]))
    def test_full_job_spec_kwargs(self, randomize_name, job_spec):
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

        yield tpl_file

    @pytest.mark.parametrize(("randomize_name",), ([True], [False]))
    def test_job_template_file(self, randomize_name, job_template_file):
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

    @pytest.mark.parametrize(("randomize_name",), ([True], [False]))
    def test_job_template_file_kwargs_override(self, randomize_name, job_template_file):
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
                r"job-a{71}-[a-z0-9]{8}",
                job.metadata.name,
            )
            is not None
        )

    def test_task_id_as_name_dag_id_is_ignored(self):
        dag = DAG(dag_id="this_is_a_dag_name", start_date=pendulum.now())
        k = KubernetesJobOperator(
            task_id="a_very_reasonable_task_name",
            dag=dag,
        )
        job = k.build_job_request_obj({})
        assert re.match(r"job-a-very-reasonable-task-name-[a-z0-9-]+", job.metadata.name) is not None
