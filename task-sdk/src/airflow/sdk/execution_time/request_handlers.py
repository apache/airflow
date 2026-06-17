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
"""
Shared request handlers for supervised subprocess comms channels.

These functions implement the supervisor-side logic for message types that are
used by more than one subprocess type (tasks, callbacks, triggerer).  Each
handler accepts a ``Client`` and a request message and returns
``(response_model | None, dump_opts)`` so the caller can forward the result
via ``send_msg``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from airflow.sdk.api.datamodels._generated import (
    AssetResponse,
    ConnectionResponse,
    DagRunStateResponse,
    TaskStatesResponse,
    VariableResponse,
    XComResponse,
    XComSequenceIndexResponse,
    XComSequenceSliceResponse,
)
from airflow.sdk.execution_time.comms import (
    AssetEventsResult,
    AssetResult,
    AssetStateStoreResult,
    ClearAssetStateStoreByName,
    ClearAssetStateStoreByUri,
    ClearTaskStateStore,
    ConnectionResult,
    CreateHITLDetailPayload,
    DagResult,
    DagRunResult,
    DagRunStateResult,
    DeleteAssetStateStoreByName,
    DeleteAssetStateStoreByUri,
    DeleteTaskStateStore,
    DeleteVariable,
    DeleteXCom,
    ErrorResponse,
    GetAssetByName,
    GetAssetByUri,
    GetAssetEventByAsset,
    GetAssetEventByAssetAlias,
    GetAssetsByAlias,
    GetAssetStateStoreByName,
    GetAssetStateStoreByUri,
    GetConnection,
    GetDag,
    GetDagRun,
    GetDagRunState,
    GetDRCount,
    GetPreviousDagRun,
    GetPreviousTI,
    GetPrevSuccessfulDagRun,
    GetTaskBreadcrumbs,
    GetTaskRescheduleStartDate,
    GetTaskStates,
    GetTaskStateStore,
    GetTICount,
    GetVariable,
    GetVariableKeys,
    GetXCom,
    GetXComCount,
    GetXComSequenceItem,
    GetXComSequenceSlice,
    HITLDetailRequestResult,
    InactiveAssetsResult,
    MaskSecret,
    OKResponse,
    PrevSuccessfulDagRunResult,
    PutVariable,
    SetAssetStateStoreByName,
    SetAssetStateStoreByUri,
    SetTaskStateStore,
    SetXCom,
    TaskBreadcrumbsResult,
    TaskStatesResult,
    TaskStateStoreResult,
    TriggerDagRun,
    ValidateInletsAndOutlets,
    VariableKeysResult,
    VariableResult,
    XComResult,
    XComSequenceIndexResult,
    XComSequenceSliceResult,
)
from airflow.sdk.log import mask_secret

if TYPE_CHECKING:
    from pydantic import BaseModel

    from airflow.sdk.api.client import Client

_HANDLER_REGISTRY: dict[type, Callable] = {}


def handles(msg_type):
    def decorator(fn):
        _HANDLER_REGISTRY[msg_type] = fn
        return fn

    return decorator


def get_handler(msg_type):
    handler = _HANDLER_REGISTRY.get(msg_type)
    if handler is None:
        raise TypeError(f"No handler registered for {msg_type.__name__}")
    return handler


@handles(GetConnection)
def handle_get_connection(client: Client, msg: GetConnection) -> tuple[BaseModel | None, dict[str, bool]]:
    """Fetch a connection and mask its sensitive fields."""
    conn = client.connections.get(msg.conn_id)
    if isinstance(conn, ConnectionResponse):
        if conn.password:
            mask_secret(conn.password)
        if conn.extra:
            mask_secret(conn.extra)
        return ConnectionResult.from_conn_response(conn), {"exclude_unset": True, "by_alias": True}
    return conn, {}


@handles(GetVariable)
def handle_get_variable(client: Client, msg: GetVariable) -> tuple[BaseModel | None, dict[str, bool]]:
    """Fetch a variable and mask its value."""
    var = client.variables.get(msg.key)
    if isinstance(var, VariableResponse):
        if var.value:
            mask_secret(var.value, var.key)
        return VariableResult.from_variable_response(var), {"exclude_unset": True}
    return var, {}


@handles(GetVariableKeys)
def handle_get_variable_keys(
    client: Client, msg: GetVariableKeys
) -> tuple[BaseModel | None, dict[str, bool]]:
    """Fetch variable keys filtered by prefix."""
    result = client.variables.keys(prefix=msg.prefix, limit=msg.limit, offset=msg.offset)
    return (
        VariableKeysResult(keys=result.keys, total_entries=result.total_entries, type="VariableKeysResult"),
        {"exclude_unset": True},
    )


@handles(MaskSecret)
def handle_mask_secret(client: Client, msg: MaskSecret) -> tuple[BaseModel | None, dict[str, bool]]:
    """Register a value with the secrets masker."""
    mask_secret(msg.value, msg.name)
    return (None, {})


@handles(PutVariable)
def handle_put_variable(client: Client, msg: PutVariable) -> tuple[BaseModel | None, dict[str, bool]]:
    """Store a variable value."""
    client.variables.set(msg.key, msg.value, msg.description)
    return None, {}


@handles(DeleteVariable)
def handle_delete_variable(client: Client, msg: DeleteVariable) -> tuple[BaseModel | None, dict[str, bool]]:
    """Delete a variable value."""
    resp = client.variables.delete(msg.key)
    return resp, {}


@handles(GetTICount)
def handle_get_ti_count(client: Client, msg: GetTICount) -> tuple[BaseModel | None, dict[str, bool]]:
    """Fetch task instance counts."""
    resp = client.task_instances.get_count(
        dag_id=msg.dag_id,
        map_index=msg.map_index,
        task_ids=msg.task_ids,
        task_group_id=msg.task_group_id,
        logical_dates=msg.logical_dates,
        run_ids=msg.run_ids,
        states=msg.states,
    )
    return resp, {}


@handles(GetTaskStates)
def handle_get_task_states(client: Client, msg: GetTaskStates) -> tuple[BaseModel | None, dict[str, bool]]:
    """Fetch task states and normalize them for supervisor response handling."""
    task_states_map = client.task_instances.get_task_states(
        dag_id=msg.dag_id,
        map_index=msg.map_index,
        task_ids=msg.task_ids,
        task_group_id=msg.task_group_id,
        logical_dates=msg.logical_dates,
        run_ids=msg.run_ids,
    )
    if isinstance(task_states_map, TaskStatesResponse):
        return TaskStatesResult.from_api_response(task_states_map), {}
    return task_states_map, {}


@handles(GetPreviousTI)
def handle_get_previous_ti(client: Client, msg: GetPreviousTI) -> tuple[BaseModel | None, dict[str, bool]]:
    """Fetch the previous task instance."""
    resp = client.task_instances.get_previous(
        dag_id=msg.dag_id,
        task_id=msg.task_id,
        logical_date=msg.logical_date,
        map_index=msg.map_index,
        state=msg.state,
    )
    return resp, {}


@handles(SetXCom)
def handle_set_xcom(client: Client, msg: SetXCom) -> tuple[BaseModel | None, dict[str, bool]]:
    """Store an XCom value."""
    client.xcoms.set(
        msg.dag_id,
        msg.run_id,
        msg.task_id,
        msg.key,
        msg.value,
        msg.map_index,
        dag_result=msg.dag_result,
        mapped_length=msg.mapped_length,
    )
    return None, {}


@handles(DeleteXCom)
def handle_delete_xcom(client: Client, msg: DeleteXCom) -> tuple[BaseModel | None, dict[str, bool]]:
    """Delete an XCom value."""
    client.xcoms.delete(msg.dag_id, msg.run_id, msg.task_id, msg.key, msg.map_index)
    return None, {}


@handles(GetDRCount)
def handle_get_dr_count(client: Client, msg: GetDRCount) -> tuple[BaseModel | None, dict[str, bool]]:
    """Fetch dag run counts."""
    resp = client.dag_runs.get_count(
        dag_id=msg.dag_id,
        logical_dates=msg.logical_dates,
        run_ids=msg.run_ids,
        states=msg.states,
    )
    return resp, {}


@handles(GetDagRunState)
def handle_get_dag_run_state(client: Client, msg: GetDagRunState) -> tuple[BaseModel | None, dict[str, bool]]:
    """Fetch dag run state."""
    dr_resp = client.dag_runs.get_state(msg.dag_id, msg.run_id)
    if isinstance(dr_resp, DagRunStateResponse):
        return DagRunStateResult.from_api_response(dr_resp), {}
    return dr_resp, {}


@handles(GetPreviousDagRun)
def handle_get_previous_dag_run(
    client: Client, msg: GetPreviousDagRun
) -> tuple[BaseModel | None, dict[str, bool]]:
    """Fetch the previous dag run."""
    resp = client.dag_runs.get_previous(
        dag_id=msg.dag_id,
        logical_date=msg.logical_date,
        state=msg.state,
    )
    return resp, {}


@handles(GetPrevSuccessfulDagRun)
def handle_get_prev_successful_dag_run(
    client: Client, msg: GetPrevSuccessfulDagRun
) -> tuple[BaseModel | None, dict[str, bool]]:
    """Fetch the previous successful dag run using the caller's current id."""
    dagrun_resp = client.task_instances.get_previous_successful_dagrun(msg.ti_id)
    dagrun_result = PrevSuccessfulDagRunResult.from_dagrun_response(dagrun_resp)
    return dagrun_result, {"exclude_unset": True}


@handles(GetXComCount)
def handle_get_xcom_count(client: Client, msg: GetXComCount) -> tuple[BaseModel | None, dict[str, bool]]:
    """Fetch XCom count metadata."""
    resp = client.xcoms.head(msg.dag_id, msg.run_id, msg.task_id, msg.key)
    return resp, {}


@handles(GetXComSequenceItem)
def handle_get_xcom_sequence_item(
    client: Client, msg: GetXComSequenceItem
) -> tuple[BaseModel | None, dict[str, bool]]:
    """Fetch an XCom sequence item and normalize it for supervisor response handling."""
    xcom = client.xcoms.get_sequence_item(msg.dag_id, msg.run_id, msg.task_id, msg.key, msg.offset)
    if isinstance(xcom, XComSequenceIndexResponse):
        return XComSequenceIndexResult.from_response(xcom), {}
    return xcom, {}


@handles(GetXComSequenceSlice)
def handle_get_xcom_sequence_slice(
    client: Client, msg: GetXComSequenceSlice
) -> tuple[BaseModel | None, dict[str, bool]]:
    """Fetch an XCom sequence slice and normalize it for supervisor response handling."""
    xcoms = client.xcoms.get_sequence_slice(
        msg.dag_id,
        msg.run_id,
        msg.task_id,
        msg.key,
        msg.start,
        msg.stop,
        msg.step,
        msg.include_prior_dates,
    )
    if isinstance(xcoms, XComSequenceSliceResponse):
        return XComSequenceSliceResult.from_response(xcoms), {}
    return xcoms, {}


@handles(GetXCom)
def handle_get_xcom(client: Client, msg: GetXCom) -> tuple[BaseModel | None, dict[str, bool]]:
    """Fetch an XCom and normalize it for supervisor response handling."""
    xcom = client.xcoms.get(
        msg.dag_id, msg.run_id, msg.task_id, msg.key, msg.map_index, msg.include_prior_dates
    )
    if isinstance(xcom, XComResponse):
        xcom_result = XComResult.from_xcom_response(xcom)
        return xcom_result, {"exclude_unset": True}
    return xcom, {}


@handles(GetAssetByName)
def handle_get_asset_by_name(client: Client, msg: GetAssetByName) -> tuple[BaseModel | None, dict[str, bool]]:
    asset_resp = client.assets.get(name=msg.name)
    if isinstance(asset_resp, AssetResponse):
        asset_result = AssetResult.from_asset_response(asset_resp)
        return asset_result, {"exclude_unset": True}
    return asset_resp, {}


@handles(GetAssetByUri)
def handle_get_asset_by_uri(client: Client, msg: GetAssetByUri) -> tuple[BaseModel | None, dict[str, bool]]:
    asset_resp = client.assets.get(uri=msg.uri)
    if isinstance(asset_resp, AssetResponse):
        asset_result = AssetResult.from_asset_response(asset_resp)
        return asset_result, {"exclude_unset": True}
    return asset_resp, {}


@handles(GetAssetsByAlias)
def handle_get_assets_by_alias(
    client: Client, msg: GetAssetsByAlias
) -> tuple[BaseModel | None, dict[str, bool]]:
    asset_resp = client.assets.get_by_alias(alias_name=msg.alias_name)
    return asset_resp, {}


@handles(GetAssetEventByAsset)
def handle_get_asset_event_by_asset(
    client: Client, msg: GetAssetEventByAsset
) -> tuple[BaseModel | None, dict[str, bool]]:
    asset_event_resp = client.asset_events.get(
        uri=msg.uri,
        name=msg.name,
        after=msg.after,
        before=msg.before,
        ascending=msg.ascending,
        limit=msg.limit,
    )
    asset_event_result = AssetEventsResult.from_asset_events_response(asset_event_resp)
    return asset_event_result, {"exclude_unset": True}


@handles(GetAssetEventByAssetAlias)
def handle_get_asset_event_by_asset_alias(
    client: Client, msg: GetAssetEventByAssetAlias
) -> tuple[BaseModel | None, dict[str, bool]]:
    asset_event_resp = client.asset_events.get(
        alias_name=msg.alias_name,
        after=msg.after,
        before=msg.before,
        ascending=msg.ascending,
        limit=msg.limit,
    )
    asset_event_result = AssetEventsResult.from_asset_events_response(asset_event_resp)
    return asset_event_result, {"exclude_unset": True}


@handles(TriggerDagRun)
def handle_trigger_dag_run(client: Client, msg: TriggerDagRun) -> tuple[BaseModel | None, dict[str, bool]]:
    resp = client.dag_runs.trigger(
        msg.dag_id,
        msg.run_id,
        msg.conf,
        msg.logical_date,
        msg.run_after,
        bool(msg.reset_dag_run),
        msg.note,
    )
    return resp, {}


@handles(GetDagRun)
def handle_get_dag_run(client: Client, msg: GetDagRun) -> tuple[BaseModel | None, dict[str, bool]]:
    dr_resp = client.dag_runs.get_detail(msg.dag_id, msg.run_id)
    resp = DagRunResult.from_api_response(dr_resp)
    return resp, {}


@handles(GetDag)
def handle_get_dag(client: Client, msg: GetDag) -> tuple[BaseModel | None, dict[str, bool]]:
    dag = client.dags.get(
        dag_id=msg.dag_id,
    )
    resp = DagResult.from_api_response(dag)
    return resp, {}


@handles(GetTaskRescheduleStartDate)
def handle_get_task_reschedule_start_date(
    client: Client, msg: GetTaskRescheduleStartDate
) -> tuple[BaseModel | None, dict[str, bool]]:
    resp = client.task_instances.get_reschedule_start_date(msg.ti_id, msg.try_number)
    return resp, {}


@handles(GetTaskBreadcrumbs)
def handle_get_task_breadcrumbs(
    client: Client, msg: GetTaskBreadcrumbs
) -> tuple[BaseModel | None, dict[str, bool]]:
    api_resp = client.task_instances.get_task_breakcrumbs(dag_id=msg.dag_id, run_id=msg.run_id)
    resp = TaskBreadcrumbsResult.from_api_response(api_resp)
    return resp, {}


@handles(ValidateInletsAndOutlets)
def handle_validate_inlets_and_outlets(
    client: Client, msg: ValidateInletsAndOutlets
) -> tuple[BaseModel | None, dict[str, bool]]:
    inactive_assets_resp = client.task_instances.validate_inlets_and_outlets(msg.ti_id)
    resp = InactiveAssetsResult.from_inactive_assets_response(inactive_assets_resp)
    return resp, {"exclude_unset": True}


@handles(CreateHITLDetailPayload)
def handle_create_hitl_detail_payload(
    client: Client, msg: CreateHITLDetailPayload
) -> tuple[BaseModel | None, dict[str, bool]]:
    hitl_detail_request = client.hitl.add_response(
        ti_id=msg.ti_id,
        options=msg.options,
        subject=msg.subject,
        body=msg.body,
        defaults=msg.defaults,
        params=msg.params,
        multiple=msg.multiple,
        assigned_users=msg.assigned_users,
    )
    resp = HITLDetailRequestResult.from_api_response(hitl_detail_request)
    return resp, {"exclude_unset": True}


@handles(GetTaskStateStore)
def handle_get_task_state_store(
    client: Client, msg: GetTaskStateStore
) -> tuple[BaseModel | None, dict[str, bool]]:
    task_state_store = client.task_state_store.get(msg.ti_id, msg.key)
    resp = (
        task_state_store
        if isinstance(task_state_store, ErrorResponse)
        else TaskStateStoreResult.from_task_state_store_response(task_state_store)
    )
    return resp, {}


@handles(SetTaskStateStore)
def handle_set_task_state_store(
    client: Client, msg: SetTaskStateStore
) -> tuple[BaseModel | None, dict[str, bool]]:
    client.task_state_store.set(msg.ti_id, msg.key, msg.value, expires_at=msg.expires_at)
    return OKResponse(ok=True), {}


@handles(DeleteTaskStateStore)
def handle_delete_task_state_store(
    client: Client, msg: DeleteTaskStateStore
) -> tuple[BaseModel | None, dict[str, bool]]:
    client.task_state_store.delete(msg.ti_id, msg.key)
    return OKResponse(ok=True), {}


@handles(ClearTaskStateStore)
def handle_clear_task_state_store(
    client: Client, msg: ClearTaskStateStore
) -> tuple[BaseModel | None, dict[str, bool]]:
    client.task_state_store.clear(msg.ti_id, all_map_indices=msg.all_map_indices)
    return OKResponse(ok=True), {}


@handles(GetAssetStateStoreByName)
def handle_get_asset_state_store_by_name(
    client: Client, msg: GetAssetStateStoreByName
) -> tuple[BaseModel | None, dict[str, bool]]:
    asset_state_store = client.asset_state_store.get(msg.key, name=msg.name)
    resp = (
        asset_state_store
        if isinstance(asset_state_store, ErrorResponse)
        else AssetStateStoreResult.from_asset_state_store_response(asset_state_store)
    )
    return resp, {}


@handles(GetAssetStateStoreByUri)
def handle_get_asset_state_store_by_uri(
    client: Client, msg: GetAssetStateStoreByUri
) -> tuple[BaseModel | None, dict[str, bool]]:
    asset_state_store = client.asset_state_store.get(msg.key, uri=msg.uri)
    resp = (
        asset_state_store
        if isinstance(asset_state_store, ErrorResponse)
        else AssetStateStoreResult.from_asset_state_store_response(asset_state_store)
    )
    return resp, {}


@handles(SetAssetStateStoreByName)
def handle_set_asset_state_store_by_name(
    client: Client, msg: SetAssetStateStoreByName
) -> tuple[BaseModel | None, dict[str, bool]]:
    client.asset_state_store.set(msg.key, msg.value, name=msg.name)
    return OKResponse(ok=True), {}


@handles(SetAssetStateStoreByUri)
def handle_set_asset_state_store_by_uri(
    client: Client, msg: SetAssetStateStoreByUri
) -> tuple[BaseModel | None, dict[str, bool]]:
    client.asset_state_store.set(msg.key, msg.value, uri=msg.uri)
    return OKResponse(ok=True), {}


@handles(DeleteAssetStateStoreByName)
def handle_delete_asset_state_store_by_name(
    client: Client, msg: DeleteAssetStateStoreByName
) -> tuple[BaseModel | None, dict[str, bool]]:
    client.asset_state_store.delete(msg.key, name=msg.name)
    return OKResponse(ok=True), {}


@handles(DeleteAssetStateStoreByUri)
def handle_delete_asset_state_store_by_uri(
    client: Client, msg: DeleteAssetStateStoreByUri
) -> tuple[BaseModel | None, dict[str, bool]]:
    client.asset_state_store.delete(msg.key, uri=msg.uri)
    return OKResponse(ok=True), {}


@handles(ClearAssetStateStoreByName)
def handle_clear_asset_state_store_by_name(
    client: Client, msg: ClearAssetStateStoreByName
) -> tuple[BaseModel | None, dict[str, bool]]:
    client.asset_state_store.clear(name=msg.name)
    return OKResponse(ok=True), {}


@handles(ClearAssetStateStoreByUri)
def handle_clear_asset_state_store_by_uri(
    client: Client, msg: ClearAssetStateStoreByUri
) -> tuple[BaseModel | None, dict[str, bool]]:
    client.asset_state_store.clear(uri=msg.uri)
    return OKResponse(ok=True), {}
