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

import builtins
import importlib.util
import json
import sys
from contextlib import nullcontext
from unittest import mock

import pytest
from kubernetes.client import CoreV1Api, models as k8s
from kubernetes.client.exceptions import ApiException

from airflow.models import TaskInstance
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.cncf.kubernetes.operators import pod as pod_module
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager, PodNotFoundException
from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred


def make_pod(name="actual-pod", namespace="actual-namespace", phase="Succeeded"):
    return k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(name=name, namespace=namespace, labels={"try_number": "1"}),
        spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="base", image="busybox")]),
        status=k8s.V1PodStatus(
            phase=phase,
            container_statuses=[
                k8s.V1ContainerStatus(
                    name="base",
                    image="busybox",
                    image_id="busybox",
                    ready=False,
                    restart_count=0,
                    state=k8s.V1ContainerState(
                        terminated=k8s.V1ContainerStateTerminated(exit_code=0 if phase == "Succeeded" else 1)
                    )
                    if phase in ("Succeeded", "Failed")
                    else k8s.V1ContainerState(running=k8s.V1ContainerStateRunning()),
                )
            ],
        ),
    )


@pytest.fixture
def context():
    ti = mock.Mock(spec=TaskInstance)
    ti.dag_id = "pod_lineage"
    ti.task_id = "workload"
    ti.try_number = 1
    ti.map_index = -1
    ti.stats_tags = {}
    ti.xcom_pull.side_effect = AssertionError("Workload identity must not be recovered from XCom")
    return {"ti": ti, "task_instance": ti, "run_id": "run"}


@pytest.fixture
def make_operator(monkeypatch):
    monkeypatch.delenv("KUBECONFIG", raising=False)

    def factory(operator_class=KubernetesPodOperator, **kwargs):
        operator = operator_class(
            **{
                "task_id": "workload",
                "name": "requested-pod",
                "namespace": "requested-namespace",
                "image": "busybox",
                "kubernetes_conn_id": None,
                "durable": False,
                "get_logs": False,
                "log_events_on_failure": False,
                "log_pod_spec_on_failure": False,
                **kwargs,
            }
        )
        actual_pod = make_pod()
        request = make_pod("requested-pod", "requested-namespace", "Pending")
        operator.pod_manager = mock.create_autospec(PodManager, instance=True)
        operator.pod_manager.create_pod.return_value = actual_pod
        operator.pod_manager.await_pod_completion.return_value = actual_pod
        operator.hook = mock.Mock(spec=KubernetesHook)
        operator.hook.get_pod.return_value = actual_pod
        operator.client = mock.Mock(spec=CoreV1Api)
        for name, return_value in (
            ("build_pod_request_obj", request),
            ("find_pod", actual_pod),
            ("await_pod_start", None),
            ("await_init_containers_completion", None),
            ("await_pod_completion", None),
            ("is_istio_enabled", False),
        ):
            monkeypatch.setattr(
                operator, name, mock.create_autospec(getattr(operator, name), return_value=return_value)
            )
        return operator

    return factory


@pytest.fixture
def extract_facets(context):
    # OpenLineage is optional for this provider, including in lowest-dependency test runs.
    extractor_module = pytest.importorskip("airflow.providers.openlineage.extractors.base")
    event_module = pytest.importorskip("openlineage.client.event_v2")
    serde_module = pytest.importorskip("openlineage.client.serde")

    def extract(operator, stage="complete"):
        extractor = extractor_module.DefaultExtractor(operator)
        if stage == "start":
            lineage = extractor.extract()
        elif stage == "failure":
            lineage = extractor.extract_on_failure(context["ti"])
        else:
            lineage = extractor.extract_on_complete(context["ti"])
        assert lineage is not None
        assert lineage.inputs == []
        assert lineage.outputs == []
        assert lineage.job_facets == {}
        run = event_module.Run(runId="00000000-0000-4000-8000-000000000000", facets=lineage.run_facets)
        return json.loads(serde_module.Serde.to_json(run))["facets"]

    return extract


def assert_workload_tags(facets, name="actual-pod", namespace="actual-namespace"):
    assert set(facets) == {"tags"}
    assert facets["tags"]["tags"] == [
        {"key": "airflow.workload.pod.name", "value": name, "source": "KubernetesPodOperator"},
        {"key": "airflow.workload.pod.namespace", "value": namespace, "source": "KubernetesPodOperator"},
    ]
    assert facets["tags"]["_schemaURL"] == (
        "https://openlineage.io/spec/facets/1-0-0/TagsRunFacet.json#/$defs/TagsRunFacet"
    )
    assert facets["tags"]["_producer"].startswith(
        "https://github.com/apache/airflow/tree/providers-openlineage/"
    )


@pytest.mark.parametrize("failed", [False, True], ids=["complete", "failure-fallback"])
def test_sync_terminal_identity_survives_cleanup(make_operator, context, extract_facets, failed):
    operator = make_operator()
    actual_pod = make_pod(phase="Failed" if failed else "Succeeded")
    operator.pod_manager.create_pod.return_value = actual_pod
    operator.pod_manager.await_pod_completion.return_value = actual_pod
    operator.find_pod.return_value = actual_pod

    assert extract_facets(operator, "start") == {}
    with pytest.raises(AirflowException) if failed else nullcontext():
        operator.execute(context)

    request = operator.build_pod_request_obj.return_value
    operator.pod_manager.create_pod.assert_called_once_with(pod=request)
    operator.pod_manager.delete_pod.assert_called_once_with(actual_pod)
    # get_or_create_pod deliberately retains its existing request-object return value.
    assert operator.pod is request
    operator.find_pod.assert_called_once_with("requested-namespace", context=context)
    actual_pod.metadata.name = "mutated-after-cleanup"
    request.metadata.namespace = "mutated-request"
    operator.pod = operator.remote_pod = operator.pod_request_obj = None
    manager_calls = list(operator.pod_manager.mock_calls)
    hook_calls = list(operator.hook.mock_calls)
    client_calls = list(operator.client.mock_calls)

    assert_workload_tags(extract_facets(operator, "failure" if failed else "complete"))
    assert extract_facets(operator, "start") == {}
    assert operator.pod_manager.mock_calls == manager_calls
    assert operator.hook.mock_calls == hook_calls
    assert operator.client.mock_calls == client_calls
    context["ti"].xcom_pull.assert_not_called()


@pytest.mark.parametrize("failed", [False, True], ids=["complete", "failure-fallback"])
def test_deferrable_terminal_identity_after_reconstruction(make_operator, context, extract_facets, failed):
    operator = make_operator(deferrable=True)
    assert extract_facets(operator, "start") == {}
    with pytest.raises(TaskDeferred) as deferred:
        operator.execute(context)
    assert deferred.value.method_name == "trigger_reentry"
    assert_workload_tags(extract_facets(operator))
    assert extract_facets(operator, "start") == {}

    resumed = make_operator(deferrable=True)
    actual_pod = make_pod(phase="Failed" if failed else "Succeeded")
    resumed.hook.get_pod.return_value = actual_pod
    resumed.pod_manager.await_pod_completion.return_value = actual_pod
    event = {
        "name": "actual-pod",
        "namespace": "actual-namespace",
        "status": "failed" if failed else "success",
        "message": "Pod finished",
    }
    with pytest.raises(AirflowException) if failed else nullcontext():
        resumed.trigger_reentry(context, event)
    resumed.hook.get_pod.assert_called_once_with("actual-pod", "actual-namespace")
    resumed.pod_manager.create_pod.assert_not_called()
    resumed.pod_manager.delete_pod.assert_called_once_with(actual_pod)
    resumed.pod = None

    assert_workload_tags(extract_facets(resumed, "failure" if failed else "complete"))


@pytest.mark.parametrize(
    ("status", "do_xcom_push", "api_status", "exception"),
    [
        ("success", False, 404, None),
        ("success", True, 404, PodNotFoundException),
        ("failed", False, 404, PodNotFoundException),
        ("error", False, 404, PodNotFoundException),
        ("timeout", False, 404, PodNotFoundException),
        ("failed", False, 503, ApiException),
    ],
)
def test_reentry_identity_precedes_pod_lookup(
    make_operator, context, extract_facets, status, do_xcom_push, api_status, exception
):
    operator = make_operator(deferrable=True, do_xcom_push=do_xcom_push)
    operator.hook.get_pod.side_effect = ApiException(status=api_status)
    event = {"name": "deleted-pod", "namespace": "deleted-namespace", "status": status}
    with pytest.raises(exception) if exception else nullcontext():
        operator.trigger_reentry(context, event)

    assert operator.pod is None
    assert_workload_tags(
        extract_facets(operator, "failure" if exception else "complete"), "deleted-pod", "deleted-namespace"
    )
    operator.hook.get_pod.assert_called_once_with("deleted-pod", "deleted-namespace")
    operator.pod_manager.create_pod.assert_not_called()
    operator.pod_manager.delete_pod.assert_not_called()


@pytest.mark.parametrize(
    ("state_store", "phase", "on_finish_action", "reused"),
    [
        (False, "Running", "delete_pod", True),
        (True, "Running", "delete_pod", True),
        (False, "Succeeded", "keep_pod", True),
        (False, "Failed", "keep_pod", True),
        (False, "Failed", "delete_pod", False),
        (True, "Succeeded", "delete_pod", False),
    ],
)
def test_identity_comes_from_actual_reattachment_or_replacement(
    make_operator, context, extract_facets, state_store, phase, on_finish_action, reused
):
    operator = make_operator(durable=True, on_finish_action=on_finish_action)
    existing = make_pod("reattached-pod", "reattached-namespace", phase)
    operator.find_pod.return_value = existing
    operator.hook.get_pod.return_value = existing
    if state_store:
        store = mock.Mock(spec=["get", "set"])
        store.get.return_value = {"name": "reattached-pod", "namespace": "reattached-namespace"}
        context["task_state_store"] = store
    request = operator.build_pod_request_obj.return_value

    result = operator.get_or_create_pod(request, context)

    assert result is (existing if reused else request)
    if reused:
        operator.pod_manager.create_pod.assert_not_called()
        assert_workload_tags(extract_facets(operator), "reattached-pod", "reattached-namespace")
    else:
        operator.pod_manager.create_pod.assert_called_once_with(pod=request)
        operator.pod_manager.delete_pod.assert_called_once_with(existing)
        assert_workload_tags(extract_facets(operator))
    if state_store:
        operator.find_pod.assert_not_called()
        operator.hook.get_pod.assert_called_once_with("reattached-pod", "reattached-namespace")


@pytest.mark.parametrize("deferrable", [False, True], ids=["sync", "deferrable"])
@pytest.mark.parametrize("next_instance", ["retry", "mapped-sibling"])
def test_failed_creation_does_not_reuse_another_attempts_identity(
    make_operator, context, extract_facets, monkeypatch, deferrable, next_instance
):
    monkeypatch.setenv("POD_NAME", "worker-pod")
    monkeypatch.setenv("POD_NAMESPACE", "worker-namespace")
    operator = make_operator(deferrable=deferrable)
    context["ti"].map_index = 0
    with pytest.raises(TaskDeferred) if deferrable else nullcontext():
        operator.execute(context)
    assert_workload_tags(extract_facets(operator))

    if next_instance == "retry":
        context["ti"].try_number += 1
        operator.pod = operator.remote_pod = None
        next_operator = operator
    else:
        context["ti"].map_index = 1
        next_operator = make_operator(deferrable=deferrable)
    next_operator.pod_manager.create_pod.side_effect = ApiException(status=403)

    with pytest.raises(ApiException):
        next_operator.execute(context)

    assert next_operator.pod_request_obj.metadata.name == "requested-pod"
    assert extract_facets(next_operator, "failure") == {}
    assert extract_facets(next_operator, "start") == {}
    context["ti"].xcom_pull.assert_not_called()
    if next_instance == "mapped-sibling":
        assert_workload_tags(extract_facets(operator))


@pytest.mark.parametrize(
    ("name", "namespace"), [(None, "namespace"), ("name", None), ("", "namespace"), ("name", "")]
)
def test_incomplete_creation_identity_is_not_reported(
    make_operator, context, extract_facets, name, namespace
):
    operator = make_operator()
    operator.pod_manager.create_pod.return_value = make_pod(name, namespace)
    operator.get_or_create_pod(operator.build_pod_request_obj.return_value, context)

    assert extract_facets(operator) == {}


def test_import_and_execution_without_optional_openlineage(make_operator, context):
    real_import = builtins.__import__

    def import_without_openlineage(name, *args, **kwargs):
        if name == "openlineage" or name.startswith(("openlineage.", "airflow.providers.openlineage")):
            raise ModuleNotFoundError(name)
        return real_import(name, *args, **kwargs)

    spec = importlib.util.spec_from_file_location("_pod_without_openlineage", pod_module.__file__)
    module = importlib.util.module_from_spec(spec)
    with (
        mock.patch.dict(sys.modules, {spec.name: module}),
        mock.patch("builtins.__import__", side_effect=import_without_openlineage),
    ):
        spec.loader.exec_module(module)
        operator = make_operator(operator_class=module.KubernetesPodOperator)
        operator.execute(context)
        operator.pod_manager.create_pod.assert_called_once()
        operator.pod_manager.delete_pod.assert_called_once()
