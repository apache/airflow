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

import typing

from sqlalchemy import select

from airflow.api_fastapi.core_api.datamodels.assets import AssetAliasResponse, AssetResponse
from airflow.cli.api_client import NEW_API_CLIENT, Client, provide_api_client
from airflow.cli.simple_table import AirflowConsole
from airflow.cli.utils import deprecated_for_airflowctl
from airflow.models.asset import AssetAliasModel, AssetModel
from airflow.utils import cli as cli_utils
from airflow.utils.session import NEW_SESSION, provide_session

if typing.TYPE_CHECKING:
    from typing import Any

    from sqlalchemy.orm import Session

    from airflow.api_fastapi.core_api.base import BaseModel


def _list_asset_aliases(args, *, session: Session) -> tuple[Any, type[BaseModel]]:
    aliases = session.scalars(select(AssetAliasModel).order_by(AssetAliasModel.name))
    return aliases, AssetAliasResponse


def _list_assets(args, *, session: Session) -> tuple[Any, type[BaseModel]]:
    assets = session.scalars(select(AssetModel).order_by(AssetModel.name)).all()
    for asset in assets:
        for watcher in asset.watchers:
            # ``AssetWatcherModel`` has no ``created_date`` column; like the public API
            # serializer, derive it from the watcher's trigger so ``AssetResponse`` validation
            # succeeds. Set on the instance so ``model_validate`` reads it via ``from_attributes``.
            watcher.created_date = watcher.trigger.created_date
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
@deprecated_for_airflowctl("airflowctl assets materialize")
@provide_api_client
def asset_materialize(args, api_client: Client = NEW_API_CLIENT) -> None:
    """
    Materialize the specified asset.

    This is done by finding the DAG with the asset defined as outlet, and create
    a run for that DAG. Resolving the DAG and creating the run is handled by the API
    server; the asset is identified here by its name and/or URI.
    """
    if not args.name and not args.uri:
        raise SystemExit("Either --name or --uri is required")

    select_message_parts = []
    if args.name:
        select_message_parts.append(f"name {args.name}")
    if args.uri:
        select_message_parts.append(f"URI {args.uri}")
    select_message = " and ".join(select_message_parts)

    matches = [
        asset
        for asset in api_client.assets.list().assets
        if (not args.name or asset.name == args.name) and (not args.uri or asset.uri == args.uri)
    ]
    if not matches:
        raise SystemExit(f"Asset with {select_message} does not exist.")
    if len(matches) > 1:
        raise SystemExit(f"More than one asset exists with {select_message}.")

    dag_run = api_client.assets.materialize(asset_id=str(matches[0].id))
    AirflowConsole().print_as(
        data=[dag_run.model_dump(mode="json")],
        output=args.output,
    )
