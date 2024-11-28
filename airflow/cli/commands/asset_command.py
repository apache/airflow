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

from airflow.api_fastapi.core_api.datamodels.assets import AssetResponse
from airflow.cli.simple_table import AirflowConsole
from airflow.models.asset import AssetModel
from airflow.utils import cli as cli_utils
from airflow.utils.session import NEW_SESSION, provide_session

if typing.TYPE_CHECKING:
    from typing import Any

    from sqlalchemy.orm import Session

log = logging.getLogger(__name__)


@cli_utils.action_cli
@provide_session
def asset_list(args, *, session: Session = NEW_SESSION) -> None:
    """Display assets in the command line."""
    assets = session.scalars(select(AssetModel).order_by(AssetModel.name))

    def detail_mapper(asset: AssetModel) -> dict[str, Any]:
        model = AssetResponse.model_validate(asset)
        return model.model_dump(include=args.columns)

    AirflowConsole().print_as(
        data=assets,
        output=args.output,
        mapper=detail_mapper,
    )


@cli_utils.action_cli
@provide_session
def asset_details(args, *, session: Session = NEW_SESSION) -> None:
    """Display details of an asset."""
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
    assets = session.scalars(stmt).all()
    select_message = " and ".join(select_message_parts)

    count = len(assets)
    if count > 1:
        raise SystemExit(f"More than one asset exists with {select_message}.")
    elif count < 1:
        raise SystemExit(f"Asset with {select_message} does not exist.")

    model_data = AssetResponse.model_validate(assets[0]).model_dump()
    if args.output in ["table", "plain"]:
        data = [{"property_name": key, "property_value": value} for key, value in model_data.items()]
    else:
        data = [model_data]

    AirflowConsole().print_as(data=data, output=args.output)
