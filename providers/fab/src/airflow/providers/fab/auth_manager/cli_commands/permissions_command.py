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
"""Permissions cleanup command."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from airflow.utils import cli as cli_utils
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.strings import to_boolean

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

log = logging.getLogger(__name__)


@provide_session
def cleanup_dag_permissions(dag_id: str, session: Session = NEW_SESSION) -> None:
    """
    Clean up DAG-specific permissions from Flask-AppBuilder tables.

    When a DAG is deleted, we need to clean up the corresponding permissions
    to prevent orphaned entries in the ab_view_menu table.

    This addresses issue #50905: Deleted DAGs not removed from ab_view_menu table
    and show up in permissions.

    :param dag_id: Specific DAG ID to clean up.
    :param session: Database session.
    """
    from sqlalchemy import delete, select

    from airflow.providers.fab.auth_manager.models import Permission, Resource, assoc_permission_role
    from airflow.security.permissions import RESOURCE_DAG_PREFIX, RESOURCE_DAG_RUN, RESOURCE_DETAILS_MAP

    # Clean up specific DAG permissions
    dag_resources = session.scalars(
        select(Resource).filter(
            Resource.name.in_(
                [
                    f"{RESOURCE_DAG_PREFIX}{dag_id}",  # DAG:dag_id
                    f"{RESOURCE_DETAILS_MAP[RESOURCE_DAG_RUN]['prefix']}{dag_id}",  # DAG_RUN:dag_id
                ]
            )
        )
    ).all()
    log.info("Cleaning up DAG-specific permissions for dag_id: %s", dag_id)

    if not dag_resources:
        return

    dag_resource_ids = [resource.id for resource in dag_resources]

    # Find all permissions associated with these resources
    dag_permissions = session.scalars(
        select(Permission).filter(Permission.resource_id.in_(dag_resource_ids))
    ).all()

    if not dag_permissions:
        # Delete resources even if no permissions exist
        session.execute(delete(Resource).where(Resource.id.in_(dag_resource_ids)))
        return

    dag_permission_ids = [permission.id for permission in dag_permissions]

    # Delete permission-role associations first (foreign key constraint)
    session.execute(
        delete(assoc_permission_role).where(
            assoc_permission_role.c.permission_view_id.in_(dag_permission_ids)
        )
    )

    # Delete permissions
    session.execute(delete(Permission).where(Permission.resource_id.in_(dag_resource_ids)))

    # Delete resources (ab_view_menu entries)
    session.execute(delete(Resource).where(Resource.id.in_(dag_resource_ids)))

    log.info("Cleaned up %d DAG-specific permissions", len(dag_permissions))


@cli_utils.action_cli
@providers_configuration_loaded
def permissions_cleanup(args):
    """Clean up DAG permissions in Flask-AppBuilder tables."""
    from sqlalchemy import select

    from airflow.models import DagModel
    from airflow.providers.fab.auth_manager.cli_commands.utils import get_application_builder
    from airflow.providers.fab.auth_manager.models import Resource
    from airflow.security.permissions import (
        RESOURCE_DAG_PREFIX,
        RESOURCE_DAG_RUN,
        RESOURCE_DETAILS_MAP,
    )
    from airflow.utils.session import create_session

    with get_application_builder() as _:
        with create_session() as session:
            # Get all existing DAG IDs from DagModel
            existing_dag_ids = {dag.dag_id for dag in session.scalars(select(DagModel)).all()}

            # Get all DAG-related resources from FAB tables
            dag_resources = session.scalars(
                select(Resource).filter(
                    Resource.name.like(f"{RESOURCE_DAG_PREFIX}%")
                    | Resource.name.like(f"{RESOURCE_DETAILS_MAP[RESOURCE_DAG_RUN]['prefix']}%")
                )
            ).all()

            orphaned_resources = []
            orphaned_dag_ids = set()

            for resource in dag_resources:
                # Extract DAG ID from resource name
                dag_id = None
                if resource.name.startswith(RESOURCE_DAG_PREFIX):
                    dag_id = resource.name[len(RESOURCE_DAG_PREFIX) :]
                elif resource.name.startswith(RESOURCE_DETAILS_MAP[RESOURCE_DAG_RUN]["prefix"]):
                    dag_id = resource.name[len(RESOURCE_DETAILS_MAP[RESOURCE_DAG_RUN]["prefix"]) :]

                # Check if this DAG ID still exists
                if dag_id and dag_id not in existing_dag_ids:
                    orphaned_resources.append(resource)
                    orphaned_dag_ids.add(dag_id)

            # Filter by specific DAG ID if provided
            if args.dag_id:
                if args.dag_id in orphaned_dag_ids:
                    orphaned_dag_ids = {args.dag_id}
                    print(f"Filtering to clean up permissions for DAG: {args.dag_id}")
                else:
                    print(
                        f"DAG '{args.dag_id}' not found in orphaned permissions or still exists in database."
                    )
                    return

            if not orphaned_dag_ids:
                if args.dag_id:
                    print(f"No orphaned permissions found for DAG: {args.dag_id}")
                else:
                    print("No orphaned DAG permissions found.")
                return

            print(f"Found orphaned permissions for {len(orphaned_dag_ids)} deleted DAG(s):")
            for dag_id in sorted(orphaned_dag_ids):
                print(f"  - {dag_id}")

            if args.dry_run:
                print("\nDry run mode: No changes will be made.")
                print(f"Would clean up permissions for {len(orphaned_dag_ids)} orphaned DAG(s).")
                return

            # Perform cleanup if not in dry run mode
            if not args.yes:
                action = (
                    f"clean up permissions for {len(orphaned_dag_ids)} DAG(s)"
                    if not args.dag_id
                    else f"clean up permissions for DAG '{args.dag_id}'"
                )
                confirm = input(f"\nDo you want to {action}? [y/N]: ")
                if not to_boolean(confirm):
                    print("Cleanup cancelled.")
                    return

            # Perform the actual cleanup
            cleanup_count = 0
            for dag_id in orphaned_dag_ids:
                try:
                    cleanup_dag_permissions(dag_id, session)
                    cleanup_count += 1
                    if args.verbose:
                        print(f"Cleaned up permissions for DAG: {dag_id}")
                except Exception as e:
                    print(f"Failed to clean up permissions for DAG {dag_id}: {e}")

            print(f"\nSuccessfully cleaned up permissions for {cleanup_count} DAG(s).")
