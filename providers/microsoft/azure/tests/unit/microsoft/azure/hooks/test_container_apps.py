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

import asyncio
import json
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime
from unittest.mock import create_autospec, patch

import pytest
import time_machine
from azure.core.exceptions import AzureError, ResourceNotFoundError
from azure.core.paging import ItemPaged
from azure.core.polling import AsyncLROPoller, LROPoller
from azure.mgmt.appcontainers import ContainerAppsAPIClient
from azure.mgmt.appcontainers.aio import ContainerAppsAPIClient as AsyncContainerAppsAPIClient
from azure.mgmt.appcontainers.aio.operations import (
    JobsExecutionsOperations as AsyncJobsExecutionsOperations,
    JobsOperations as AsyncJobsOperations,
)
from azure.mgmt.appcontainers.models import (
    Container,
    ContainerResources,
    EnvironmentVar,
    InitContainer,
    Job,
    JobExecution,
    JobExecutionBase,
    JobExecutionRunningState,
    JobTemplate,
)
from azure.mgmt.appcontainers.operations import JobsExecutionsOperations, JobsOperations

from airflow.exceptions import TaskDeferred
from airflow.models import Connection
from airflow.providers.microsoft.azure.exceptions import AzureContainerAppsJobExecutionError
from airflow.providers.microsoft.azure.hooks.container_apps import (
    AzureContainerAppsJobAsyncHook,
    AzureContainerAppsJobHook,
    build_execution_template,
    get_execution_result,
)
from airflow.providers.microsoft.azure.operators.container_apps import AzureContainerAppsJobOperator
from airflow.providers.microsoft.azure.triggers.container_apps import AzureContainerAppsJobTrigger

MODULE = "airflow.providers.microsoft.azure.hooks.container_apps"


@pytest.fixture
def client():
    client = create_autospec(ContainerAppsAPIClient, instance=True)
    client.jobs = create_autospec(JobsOperations, instance=True)
    client.jobs_executions = create_autospec(JobsExecutionsOperations, instance=True)
    poller = create_autospec(LROPoller, instance=True)
    poller.result.return_value = JobExecutionBase(name="execution")
    client.jobs.begin_start.return_value = poller
    return client


@pytest.fixture
def hook(client):
    hook = AzureContainerAppsJobHook(subscription_id="subscription")

    @contextmanager
    def get_client(**kwargs):
        yield client

    with patch.object(hook, "get_client", autospec=True, side_effect=get_client):
        yield hook


@pytest.mark.parametrize(("submission", "expected_retries"), [(False, 3), (True, 0)])
@pytest.mark.parametrize("service_principal", [False, True])
@patch(f"{MODULE}.ContainerAppsAPIClient", autospec=True)
@patch(f"{MODULE}.ClientSecretCredential", autospec=True)
@patch(f"{MODULE}.get_sync_default_azure_credential", autospec=True)
def test_client_auth_and_cleanup(
    default_credential,
    secret_credential,
    sdk_client,
    create_mock_connection,
    service_principal,
    submission,
    expected_retries,
):
    create_mock_connection(
        Connection(
            conn_id="azure_default",
            conn_type="azure",
            login="client" if service_principal else None,
            password="secret" if service_principal else None,
            extra={
                "tenantId": "tenant",
                "subscriptionId": "connection-subscription",
                "managed_identity_client_id": "identity",
                "workload_identity_tenant_id": "workload-tenant",
            },
        )
    )
    hook = AzureContainerAppsJobHook(subscription_id="explicit-subscription")
    with pytest.raises(RuntimeError), hook.get_client(submission=submission):
        raise RuntimeError("consumer failed")
    if service_principal:
        secret_credential.assert_called_once_with(
            tenant_id="tenant", client_id="client", client_secret="secret"
        )
        credential = secret_credential.return_value
    else:
        default_credential.assert_called_once_with(
            managed_identity_client_id="identity", workload_identity_tenant_id="workload-tenant"
        )
        credential = default_credential.return_value
    assert sdk_client.call_args.kwargs["subscription_id"] == "explicit-subscription"
    policy = sdk_client.call_args.kwargs["retry_policy"]
    assert policy.total_retries == expected_retries
    assert policy.connect_retries == policy.read_retries == policy.status_retries == expected_retries
    credential.__exit__.assert_called_once()
    sdk_client.return_value.__exit__.assert_called_once()


def test_subscription_resolution():
    connection = Connection(conn_id="azure_default", extra={"subscriptionId": "from-connection"})
    assert AzureContainerAppsJobHook().get_subscription_id(connection) == "from-connection"
    with pytest.raises(ValueError, match="subscription_id"):
        AzureContainerAppsJobHook().get_subscription_id(Connection(conn_id="empty"))


def test_submit_defaults_once(hook, client):
    assert hook.start_job("group", "job") == "execution"
    hook.get_client.assert_called_once_with(submission=True)
    client.jobs.get.assert_not_called()
    client.jobs.begin_start.assert_called_once_with(
        resource_group_name="group", job_name="job", template=None
    )


@pytest.mark.parametrize("cpu", ["{{ params.cpu }}", 1, 0.5, None])
def test_operator_submits_numeric_cpu_after_rendering(hook, client, cpu):
    client.jobs.get.return_value = Job(
        location="westeurope",
        template=JobTemplate(containers=[Container(name="worker", resources=ContainerResources(cpu=0.25))]),
    )
    operator = AzureContainerAppsJobOperator(
        task_id="job",
        resource_group="group",
        job_name="job",
        container_name="worker",
        cpu=cpu,
        memory="1Gi",
        deferrable=True,
    )
    operator.hook = hook
    operator.render_template_fields({"params": {"cpu": "0.5"}})
    with pytest.raises(TaskDeferred):
        operator.execute({})
    submitted = client.jobs.begin_start.call_args.kwargs["template"]
    payload = json.loads(json.dumps(submitted.as_dict()))
    actual = payload["containers"][0]["resources"]["cpu"]
    assert actual == (0.25 if cpu is None else 0.5 if isinstance(cpu, str) else cpu)
    assert isinstance(actual, (float, int))


@pytest.mark.parametrize("error", [AzureError("ambiguous response"), RuntimeError("unexpected")])
def test_submit_error_is_not_retried(hook, client, error):
    client.jobs.begin_start.side_effect = error
    with pytest.raises(type(error), match=str(error)):
        hook.start_job("group", "job")
    assert client.jobs.begin_start.call_count == 1


def test_submit_without_execution_name_fails(hook, client):
    client.jobs.begin_start.return_value.result.return_value = JobExecutionBase()
    with pytest.raises(AzureContainerAppsJobExecutionError, match="no execution name"):
        hook.start_job("group", "job")


def test_overrides_preserve_other_containers(hook, client):
    template = JobTemplate(
        containers=[
            Container(name="sidecar", image="sidecar-image"),
            Container(
                name="worker",
                image="old",
                command=["old-command"],
                args=["old-arg"],
                env=[EnvironmentVar(name="SECRET", secret_ref="secret")],
                resources=ContainerResources(cpu=0.5, memory="1Gi"),
            ),
        ],
        init_containers=[InitContainer(name="init", image="init-image", command=["prepare"])],
    )
    client.jobs.get.return_value = Job(location="westeurope", template=template)
    assert (
        hook.start_job(
            "group",
            "job",
            container_name="worker",
            image="new",
            command=[],
            args=["new-arg"],
            env_vars={"NEW": "value"},
            cpu=1,
        )
        == "execution"
    )
    submitted = client.jobs.begin_start.call_args.kwargs["template"]
    assert submitted.containers[0].image == "sidecar-image"
    assert submitted.init_containers[0].command == ["prepare"]
    worker = submitted.containers[1]
    assert (worker.image, worker.command, worker.args) == ("new", [], ["new-arg"])
    assert [(env.name, env.value) for env in worker.env] == [("NEW", "value")]
    assert worker.resources.cpu == 1
    assert worker.resources.memory == "1Gi"
    assert template.containers[1].image == "old"
    assert template.containers[1].env[0].secret_ref == "secret"
    client.jobs.begin_start.assert_called_once()


def test_partial_overrides_and_empty_environment():
    template = JobTemplate(containers=[Container(name="worker", image="old")])
    result = build_execution_template(template, "worker", memory="2Gi", env_vars={})
    assert result.containers[0].resources.memory == "2Gi"
    assert result.containers[0].env == []
    assert result.containers[0].image == "old"


def test_image_override_preserves_environment_and_resources():
    template = JobTemplate(
        containers=[
            Container(
                name="worker",
                image="old",
                env=[EnvironmentVar(name="SECRET", secret_ref="secret")],
                resources=ContainerResources(cpu=0.5, memory="1Gi"),
            )
        ]
    )
    result = build_execution_template(template, "worker", image="new")
    assert result.containers[0].image == "new"
    assert result.containers[0].env[0].secret_ref == "secret"
    assert result.containers[0].resources == template.containers[0].resources


@pytest.mark.parametrize(
    ("container_name", "template"), [(None, JobTemplate()), ("missing", JobTemplate()), ("worker", None)]
)
def test_invalid_override_target_does_not_submit(hook, client, container_name, template):
    client.jobs.get.return_value = Job(location="westeurope", template=template)
    with pytest.raises(ValueError, match="container|template"):
        hook.start_job("group", "job", container_name=container_name, image="new")
    client.jobs.begin_start.assert_not_called()


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (None, None),
        ("", None),
        ("Running", None),
        ("Processing", None),
        ("Unknown", None),
        (JobExecutionRunningState.RUNNING, None),
        (" succeeded ", "success"),
        ("Failed", "error"),
        ("Stopped", "error"),
        ("new-terminal-state", "error"),
    ],
)
def test_classification(status, expected):
    result = get_execution_result("execution", JobExecution(status=status))
    assert (result["status"] if result else None) == expected


@pytest.mark.parametrize(
    ("message", "reason", "diagnostic"),
    [("message", "reason", "message"), (None, "reason", "reason"), (None, None, "Failed")],
)
def test_failure_diagnostics(message, reason, diagnostic):
    result = get_execution_result("execution", JobExecution(status="Failed", message=message, reason=reason))
    assert diagnostic in result["message"]


@pytest.mark.parametrize("found", [True, False])
def test_missing_lookup_uses_exact_history_name(hook, client, found):
    client.job_execution.side_effect = ResourceNotFoundError()
    exact = JobExecution({"name": "execution", "properties": {"status": "Succeeded"}})
    client.jobs_executions.list.return_value = ItemPaged(
        lambda token: None,
        lambda response: (None, iter([JobExecution({"name": "other"})] + ([exact] if found else []))),
    )
    result = hook.get_execution("group", "job", "execution")
    assert result is exact if found else result is None
    if not found:
        assert get_execution_result("execution", result)["status"] == "error"


@patch(f"{MODULE}.time", autospec=True)
def test_lookup_refreshes_budget_for_history_pages(clock, hook, client):
    clock.monotonic.side_effect = [0, 1, 3, 7]
    client.job_execution.side_effect = ResourceNotFoundError()
    exact = JobExecution({"name": "execution"})
    tokens = []

    def get_page(token):
        tokens.append(token)
        return ("next", iter([JobExecution({"name": "other"})])) if token is None else (None, iter([exact]))

    client.jobs_executions.list.side_effect = lambda **kwargs: ItemPaged(get_page, lambda response: response)
    assert hook.get_execution("group", "job", "execution", timeout_seconds=10) is exact
    assert tokens == [None, "next"]
    assert client.job_execution.call_args.kwargs == {
        "resource_group_name": "group",
        "job_name": "job",
        "job_execution_name": "execution",
        "timeout": 9,
        "connection_timeout": 9,
        "read_timeout": 9,
    }
    assert [call.kwargs for call in client.jobs_executions.list.call_args_list] == [
        {
            "resource_group_name": "group",
            "job_name": "job",
            "timeout": budget,
            "connection_timeout": budget,
            "read_timeout": budget,
        }
        for budget in [7, 3]
    ]


@pytest.mark.parametrize("ticks", [[0, 10], [0, 1, 10], [0, 1, 3, 10]])
@patch(f"{MODULE}.time", autospec=True)
def test_lookup_deadline_prevents_further_requests(clock, hook, client, ticks):
    clock.monotonic.side_effect = ticks
    client.job_execution.side_effect = ResourceNotFoundError()
    client.jobs_executions.list.side_effect = lambda **kwargs: ItemPaged(
        lambda token: ("next", iter([JobExecution({"name": "other"})])), lambda response: response
    )
    with pytest.raises(TimeoutError, match="lookup timed out"):
        hook.get_execution("group", "job", "execution", timeout_seconds=10)
    assert client.job_execution.call_count == (len(ticks) > 2)
    assert client.jobs_executions.list.call_count == max(0, len(ticks) - 3)


@pytest.mark.parametrize("ticks", [[0, 1, 10], [0, 1, 3, 10]])
@patch("airflow.providers.microsoft.azure.operators.container_apps.time", autospec=True)
@patch(f"{MODULE}.time", autospec=True)
def test_worker_timeout_during_history_stops_submitted_execution(
    hook_clock, worker_clock, hook, client, ticks
):
    hook_clock.monotonic.side_effect = ticks
    worker_clock.monotonic.side_effect = [0, 0, 10]
    client.job_execution.side_effect = ResourceNotFoundError()
    client.jobs_executions.list.side_effect = lambda **kwargs: ItemPaged(
        lambda token: ("next", iter([JobExecution({"name": "other"})])), lambda response: response
    )
    operator = AzureContainerAppsJobOperator(
        task_id="job",
        resource_group="group",
        job_name="job",
        timeout_seconds=10,
        cancel_on_termination=False,
    )
    operator.hook = hook
    with pytest.raises(AzureContainerAppsJobExecutionError, match="timed out"):
        operator.execute({})
    client.jobs.begin_start.assert_called_once()
    client.jobs.begin_stop_execution.assert_called_once_with(
        resource_group_name="group", job_name="job", job_execution_name="execution"
    )
    worker_clock.sleep.assert_not_called()


def test_direct_lookup_and_exact_stop(hook, client):
    assert hook.get_execution("group", "job", "execution") is client.job_execution.return_value
    hook.stop_execution("group", "job", "execution")
    client.jobs.begin_stop_execution.assert_called_once_with(
        resource_group_name="group", job_name="job", job_execution_name="execution"
    )
    client.jobs_executions.list.assert_not_called()


def test_lookup_errors_propagate(hook, client):
    client.job_execution.side_effect = AzureError("lookup failed")
    with pytest.raises(AzureError, match="lookup failed"):
        hook.get_execution("group", "job", "execution")
    client.jobs_executions.list.assert_not_called()


@pytest.fixture
def async_client():
    client = create_autospec(AsyncContainerAppsAPIClient, instance=True)
    client.jobs = create_autospec(AsyncJobsOperations, instance=True)
    client.jobs_executions = create_autospec(AsyncJobsExecutionsOperations, instance=True)
    poller = create_autospec(AsyncLROPoller, instance=True)
    client.jobs.begin_stop_execution.return_value = poller
    return client


@pytest.fixture
def async_hook(async_client):
    hook = AzureContainerAppsJobAsyncHook(subscription_id="subscription")

    @asynccontextmanager
    async def get_client():
        yield async_client

    with patch.object(hook, "get_async_client", autospec=True, side_effect=get_client):
        yield hook


@pytest.mark.asyncio
@pytest.mark.parametrize("found", [True, False])
async def test_async_history_fallback(async_hook, async_client, found):
    async_client.job_execution.side_effect = ResourceNotFoundError()
    exact = JobExecution({"name": "execution", "properties": {"status": "Failed"}})

    async def history():
        yield JobExecution({"name": "other"})
        if found:
            yield exact

    async_client.jobs_executions.list.return_value = history()
    result = await async_hook.async_get_execution("group", "job", "execution")
    assert result is exact if found else result is None


@pytest.mark.asyncio
@pytest.mark.parametrize("history", [False, True])
@time_machine.travel(datetime(2026, 1, 1, tzinfo=UTC), tick=False)
@patch("airflow.providers.microsoft.azure.triggers.container_apps.time", autospec=True)
@patch("airflow.providers.microsoft.azure.triggers.container_apps.asyncio.timeout", autospec=True)
@patch(
    "airflow.providers.microsoft.azure.triggers.container_apps.AzureContainerAppsJobAsyncHook", autospec=True
)
async def test_deferred_deadline_interrupts_complete_lookup(
    hook_factory, timeout_factory, clock, async_hook, async_client, history
):
    clock.monotonic.return_value = 0
    hook_factory.return_value = async_hook
    timeout = asyncio.Timeout(None)
    timeout_factory.return_value = timeout
    cancelled = asyncio.Event()

    async def block():
        timeout.reschedule(asyncio.get_running_loop().time())
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    if history:
        async_client.job_execution.side_effect = ResourceNotFoundError()

        async def pages():
            yield JobExecution({"name": "other"})
            await block()

        async_client.jobs_executions.list.return_value = pages()
    else:

        async def lookup(**kwargs):
            await block()

        async_client.job_execution.side_effect = lookup

    trigger = AzureContainerAppsJobTrigger(
        resource_group="group",
        job_name="job",
        execution_name="execution",
        deadline="2026-01-01T00:01:00+00:00",
        cancel_on_termination=False,
    )
    events = [event async for event in trigger.run()]
    assert cancelled.is_set()
    assert len(events) == 1
    assert events[0].payload["status"] == "error"
    assert "timed out" in events[0].payload["message"]
    timeout_factory.assert_called_once_with(60)
    async_client.jobs.begin_stop_execution.assert_awaited_once_with(
        resource_group_name="group", job_name="job", job_execution_name="execution"
    )


@pytest.mark.asyncio
async def test_async_lookup_stop_and_errors(async_hook, async_client):
    assert (
        await async_hook.async_get_execution("group", "job", "execution")
        is async_client.job_execution.return_value
    )
    await async_hook.async_stop_execution("group", "job", "execution")
    async_client.jobs.begin_stop_execution.assert_awaited_once_with(
        resource_group_name="group", job_name="job", job_execution_name="execution"
    )
    async_client.jobs.begin_stop_execution.return_value.result.assert_awaited_once()
    async_client.job_execution.side_effect = RuntimeError("unexpected")
    with pytest.raises(RuntimeError, match="unexpected"):
        await async_hook.async_get_execution("group", "job", "execution")


@pytest.mark.asyncio
@pytest.mark.parametrize("service_principal", [False, True])
@patch(f"{MODULE}.AsyncContainerAppsAPIClient", autospec=True)
@patch(f"{MODULE}.AsyncClientSecretCredential", autospec=True)
@patch(f"{MODULE}.get_async_default_azure_credential", autospec=True)
@patch(f"{MODULE}.get_async_connection", autospec=True)
async def test_async_auth_and_cleanup(
    connection, default_credential, secret_credential, sdk_client, service_principal
):
    connection.return_value = Connection(
        conn_id="azure_default",
        login="client" if service_principal else None,
        password="secret" if service_principal else None,
        extra={"tenantId": "tenant", "subscriptionId": "subscription"},
    )
    hook = AzureContainerAppsJobAsyncHook()
    with pytest.raises(RuntimeError):
        async with hook.get_async_client():
            raise RuntimeError("consumer failed")
    if service_principal:
        secret_credential.assert_called_once_with(
            tenant_id="tenant", client_id="client", client_secret="secret"
        )
        credential = secret_credential.return_value
    else:
        default_credential.assert_called_once_with(
            managed_identity_client_id=None, workload_identity_tenant_id=None
        )
        credential = default_credential.return_value
    assert sdk_client.call_args.kwargs["retry_policy"].total_retries == 3
    credential.__aexit__.assert_awaited_once()
    sdk_client.return_value.__aexit__.assert_awaited_once()
