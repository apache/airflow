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

import logging
import typing

from sqlalchemy import select

from airflow.api.common.trigger_dag import trigger_dag
from airflow.api_fastapi.core_api.datamodels.assets import AssetAliasResponse, AssetResponse
from airflow.api_fastapi.core_api.datamodels.dag_run import DAGRunResponse
from airflow.cli.simple_table import AirflowConsole
from airflow.exceptions import AirflowConfigException
from airflow.models.asset import AssetAliasModel, AssetModel, TaskOutletAssetReference
from airflow.utils import cli as cli_utils
from airflow.utils.platform import getuser
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.types import DagRunTriggeredByType

if typing.TYPE_CHECKING:
    from typing import Any

    from sqlalchemy.orm import Session

    from airflow.api_fastapi.core_api.base import BaseModel

log = logging.getLogger(__name__)


def _list_asset_aliases(args, *, session: Session) -> tuple[Any, type[BaseModel]]:
    aliases = session.scalars(select(AssetAliasModel).order_by(AssetAliasModel.name))
    return aliases, AssetAliasResponse


def _list_assets(args, *, session: Session) -> tuple[Any, type[BaseModel]]:
    assets = session.scalars(select(AssetModel).order_by(AssetModel.name))
    return assets, AssetResponse


@cli_utils.action_cli
@provide_session
def asset_list(args, *, session: Session = NEW_SESSION) -> None:
    """Display assets in the command line."""
    if args.alias:
        data, model_cls = _list_asset_aliases(args, session=session)
    else:
        data, model_cls = _list_assets(args, session=session)

    def detail_mapper(asset: Any) -> dict[str, Any]:
        model = model_cls.model_validate(asset)
        return model.model_dump(mode="json", include=args.columns)

    AirflowConsole().print_as(data=data, output=args.output, mapper=detail_mapper)


def _detail_asset_alias(args, *, session: Session) -> BaseModel:
    if not args.name:
        raise SystemExit("Required --name with --alias")
    if args.uri:
        raise SystemExit("Cannot use --uri with --alias")

    alias = session.scalar(select(AssetAliasModel).where(AssetAliasModel.name == args.name))
    if alias is None:
        raise SystemExit(f"Asset alias with name {args.name} does not exist.")

    return AssetAliasResponse.model_validate(alias)


def _detail_asset(args, *, session: Session) -> BaseModel:
    if not args.name and not args.uri:
        raise SystemExit("Either --name or --uri is required")

    stmt = select(AssetModel)
    select_message_parts = []
    if args.name:
        stmt = stmt.where(AssetModel.name == args.name)
        select_message_parts.append(f"name {args.name}")
    if args.uri:
        stmt = stmt.where(AssetModel.uri == args.uri)
        select_message_parts.append(f"URI {args.uri}")
    asset_it = iter(session.scalars(stmt.limit(2)))
    select_message = " and ".join(select_message_parts)

    if (asset := next(asset_it, None)) is None:
        raise SystemExit(f"Asset with {select_message} does not exist.")
    if next(asset_it, None) is not None:
        raise SystemExit(f"More than one asset exists with {select_message}.")

    return AssetResponse.model_validate(asset)


@cli_utils.action_cli
@provide_session
def asset_details(args, *, session: Session = NEW_SESSION) -> None:
    """Display details of an asset."""
    if args.alias:
        model = _detail_asset_alias(args, session=session)
    else:
        model = _detail_asset(args, session=session)

    model_data = model.model_dump(mode="json")
    if args.output in ["table", "plain"]:
        data = [{"property_name": key, "property_value": value} for key, value in model_data.items()]
    else:
        data = [model_data]

    AirflowConsole().print_as(data=data, output=args.output)


@cli_utils.action_cli
@provide_session
def asset_materialize(args, *, session: Session = NEW_SESSION) -> None:
    """
    Materialize the specified asset.

    This is done by finding the DAG with the asset defined as outlet, and create
    a run for that DAG.
    """
    if not args.name and not args.uri:
        raise SystemExit("Either --name or --uri is required")

    stmt = select(TaskOutletAssetReference.dag_id).join(TaskOutletAssetReference.asset)
    select_message_parts = []
    if args.name:
        stmt = stmt.where(AssetModel.name == args.name)
        select_message_parts.append(f"name {args.name}")
    if args.uri:
        stmt = stmt.where(AssetModel.uri == args.uri)
        select_message_parts.append(f"URI {args.uri}")
    dag_id_it = iter(session.scalars(stmt.group_by(TaskOutletAssetReference.dag_id).limit(2)))
    select_message = " and ".join(select_message_parts)

    if (dag_id := next(dag_id_it, None)) is None:
        raise SystemExit(f"Asset with {select_message} does not exist.")
    if next(dag_id_it, None) is not None:
        raise SystemExit(f"More than one DAG materializes asset with {select_message}.")

    try:
        user = getuser()
    except AirflowConfigException as e:
        log.warning("Failed to get user name from os: %s, not setting the triggering user", e)
        user = None
    dagrun = trigger_dag(
        dag_id=dag_id, triggered_by=DagRunTriggeredByType.CLI, triggering_user_name=user, session=session
    )
    if dagrun is not None:
        data = [DAGRunResponse.model_validate(dagrun).model_dump(mode="json")]
    else:
        data = []

    AirflowConsole().print_as(data=data, output=args.output)
