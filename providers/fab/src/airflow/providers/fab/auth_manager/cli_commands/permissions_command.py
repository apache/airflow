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

from airflow.utils import cli as cli_utils
from airflow.utils.providers_configuration_loader import providers_configuration_loaded


@cli_utils.action_cli
@providers_configuration_loaded
def permissions_cleanup(args):
    """Clean up DAG permissions in Flask-AppBuilder tables."""
    from sqlalchemy import select

    from airflow.models import DagModel
    from airflow.providers.fab.auth_manager.cli_commands.utils import get_application_builder
    from airflow.providers.fab.auth_manager.models import Resource
    from airflow.security.permissions import RESOURCE_DAG_PREFIX
    from airflow.utils.session import create_session

    with get_application_builder() as _:
        with create_session() as session:
            # Get all existing DAG IDs from DagModel
            existing_dag_ids = {dag.dag_id for dag in session.scalars(select(DagModel)).all()}

            # Get all DAG-related resources from FAB tables
            dag_resources = session.scalars(
                select(Resource).filter(
                    Resource.name.like(f"{RESOURCE_DAG_PREFIX}%")
                    | Resource.name.like("DAG Run:%")
                    | Resource.name.like("Task Instance:%")
                )
            ).all()

            orphaned_resources = []
            orphaned_dag_ids = set()

            for resource in dag_resources:
                # Extract DAG ID from resource name
                dag_id = None
                if resource.name.startswith(RESOURCE_DAG_PREFIX):
                    dag_id = resource.name[len(RESOURCE_DAG_PREFIX) :]
                elif resource.name.startswith("DAG Run:"):
                    dag_id = resource.name[len("DAG Run:") :]
                elif resource.name.startswith("Task Instance:"):
                    dag_id = resource.name[len("Task Instance:") :]

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
                if confirm.lower() not in ("y", "yes"):
                    print("Cleanup cancelled.")
                    return

            # Perform the actual cleanup
            from airflow.providers.fab.auth_manager.dag_permissions import cleanup_dag_permissions

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
