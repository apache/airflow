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
import argparse
import datetime
import sys
from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any

import airflow_google_provider_resource_cleanup.constants as constants
from airflow_google_provider_resource_cleanup.handlers import get_delete_handlers
from airflow_google_provider_resource_cleanup.helpers import (
    GCPProjectConfig,
    check_white_list,
    get_auxiliary_asset_data_item,
    get_resources_file,
    load_json,
    provide_auxiliary_asset_resource_file,
)

RESOURCE_CREATE_TIME_FIELDS = ("createTime", "creationTimestamp")


def _parse_datetime(value: str) -> datetime.datetime:
    dt = datetime.datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc)


def _parse_create_time(value: Any) -> datetime.datetime | None:
    if not value:
        return None
    try:
        return _parse_datetime(str(value))
    except ValueError:
        return None


def _get_resource_create_time(resource: Mapping[str, Any]) -> datetime.datetime | None:
    locations: list[Mapping[str, Any]] = [resource]

    # Some resources have the create time nested in the additionalAttributes field.
    additional_attributes = resource.get("additionalAttributes", {})
    if isinstance(additional_attributes, Mapping):
        locations.append(additional_attributes)

    for location in locations:
        for field in RESOURCE_CREATE_TIME_FIELDS:
            create_time = _parse_create_time(location.get(field))
            if create_time:
                return create_time

    return None


def check_min_age(
    resource: Mapping[str, Any],
    min_age_days: int | None,
    now: datetime.datetime | None = None,
) -> bool:
    if min_age_days is None:
        return True

    created_at = _get_resource_create_time(resource)
    resource_name = resource.get("name", "N/a")
    if created_at is None:
        print(
            f'The resource: "{resource_name}" does not have a known creation time. '
            f"Skipping because --min-age-days={min_age_days} is enabled."
        )
        return False

    now = now or datetime.datetime.now(datetime.timezone.utc)
    age = now - created_at
    if age < datetime.timedelta(days=min_age_days):
        print(
            f'The resource: "{resource_name}" is younger than {min_age_days} day(s). '
            f"Created at: {created_at.isoformat()}. Skipping."
        )
        return False

    return True


async def handle_asset_type(
    asset_type: str,
    resources: Sequence[Mapping[str, Any]],
    cfg: GCPProjectConfig,
    min_age_days: int | None = None,
    check_white_list_fn=check_white_list,
    delete_handlers=None,
):
    delete_handlers = delete_handlers or get_delete_handlers()
    safe_resources = []
    for resource in resources:
        if not check_white_list_fn(resource, cfg):
            continue
        if not check_min_age(resource, min_age_days):
            continue
        safe_resources.append(resource)

    if not safe_resources:
        print(f'No safe resources found for the asset type: "{asset_type}"! Passing...')
        return

    if asset_type not in delete_handlers:
        print(f'There is no delete handler implemented for the asset type: "{asset_type}"! Passing...')
        return

    if asset_type not in constants.AUXILIARY_ASSET_TYPES:
        print(f'{len(resources)} resources found for the asset type: "{asset_type}"!. Deleting...')
    else:
        print(f"Auxiliary resource cleanup requested: '{asset_type}'. Deleting attempt...")

    handler = delete_handlers[asset_type]()

    await handler.handle(safe_resources)


async def handle_delete(
    _: argparse.ArgumentParser,
    args: argparse.Namespace,
    gcp_project_config_cls=GCPProjectConfig,
    provide_auxiliary_asset_resource_file_fn=provide_auxiliary_asset_resource_file,
    get_resources_file_fn=get_resources_file,
    load_json_fn=load_json,
    get_auxiliary_asset_data_item_fn=get_auxiliary_asset_data_item,
    handle_asset_type_fn=handle_asset_type,
):
    asset_type = args.asset_type
    project_id = args.project_id
    min_age_days = args.min_age_days
    skip_asset_types = args.skip_asset_type

    if min_age_days is not None and min_age_days < 0:
        print("--min-age-days cannot be negative! Exiting...")
        sys.exit(1)

    if asset_type in skip_asset_types:
        print(f'The requested asset type "{asset_type}" is listed in --skip-asset-type. Passing...')
        return

    cfg = gcp_project_config_cls.load_from_file(project_id, config_path=args.config_path)

    if asset_type in constants.AUXILIARY_ASSET_TYPES:
        provide_auxiliary_asset_resource_file_fn(
            project_id,
            asset_type,
            resource_file_path=args.resources_file_path,
            config_path=args.config_path,
        )

    resource_file = get_resources_file_fn(project_id, asset_type, resource_file_path=args.resources_file_path)
    if not resource_file.exists():
        print(f'The resources file cannot be found: "{resource_file.absolute()}"! Exiting...')
        print('You may need to run "list" command first to retrieve the updated resources! Exiting...')
        sys.exit(1)
    print(f'The resources file loading: "{resource_file.absolute()}"...')
    resources = load_json_fn(resource_file)

    categorized_resources = defaultdict(list)

    if asset_type not in constants.AUXILIARY_ASSET_TYPES:
        for _asset_type in constants.ASSET_TYPES:
            _asset_type_pattern = constants.ASSET_TYPES[_asset_type]
            _asset_type_value = ".".join(_asset_type_pattern.split(".")[:-1])
            for resource in resources:
                if resource.get("assetType").startswith(_asset_type_value):
                    categorized_resources[_asset_type].append(resource)
    else:
        for _asset_type in constants.AUXILIARY_ASSET_TYPES:
            _asset_type_pattern = constants.AUXILIARY_ASSET_TYPES[_asset_type]
            _asset_type_value = ".".join(_asset_type_pattern.split(".")[:-1])

            for resource in resources:
                if resource.get("assetType").startswith(_asset_type_value):
                    categorized_resources[_asset_type].append(resource)

    if asset_type is not None:
        resource_list = categorized_resources[asset_type]
        await handle_asset_type_fn(asset_type, resource_list, cfg, min_age_days=min_age_days)
    else:  # clean all resources
        # Append synthetic auxiliary resources that are not returned by Cloud Asset Inventory.
        categorized_resources["vertex_ai_raycluster"] = [
            get_auxiliary_asset_data_item_fn(project_id, asset_type="vertex_ai_raycluster")
        ]
        for asset_type, resource_list in categorized_resources.items():
            if asset_type in skip_asset_types:
                print(f'The asset type "{asset_type}" is listed in --skip-asset-type. Passing...')
                continue
            await handle_asset_type_fn(asset_type, resource_list, cfg, min_age_days=min_age_days)
