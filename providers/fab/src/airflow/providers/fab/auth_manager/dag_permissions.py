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
"""DAG permissions management for FAB Auth Manager."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from airflow.security.permissions import RESOURCE_DAG_PREFIX

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

log = logging.getLogger(__name__)


def cleanup_dag_permissions(dag_id: str, session: Session | None = None) -> None:
    """
    Clean up DAG-specific permissions from Flask-AppBuilder tables.

    When a DAG is deleted, we need to clean up the corresponding permissions
    to prevent orphaned entries in the ab_view_menu table.

    This addresses issue #50905: Deleted DAGs not removed from ab_view_menu table
    and show up in permissions.

    :param dag_id: Specific DAG ID to clean up.
    :param session: Database session. If None, creates a new session.
    """
    from airflow.utils.session import create_session

    if session is None:
        with create_session() as session:
            _cleanup_dag_permissions_impl(dag_id, session)
    else:
        _cleanup_dag_permissions_impl(dag_id, session)


def _cleanup_dag_permissions_impl(dag_id: str, session: Session) -> None:
    """Implement DAG permissions cleanup."""
    from sqlalchemy import select

    from airflow.providers.fab.auth_manager.models import Permission, Resource, assoc_permission_role

    # Clean up specific DAG permissions
    dag_resource_name = f"{RESOURCE_DAG_PREFIX}{dag_id}"
    dag_resources = session.scalars(
        select(Resource).filter(
            Resource.name.in_(
                [
                    dag_resource_name,  # DAG:dag_id
                    f"DAG Run:{dag_id}",  # DAG_RUN:dag_id
                    f"Task Instance:{dag_id}",  # TASK_INSTANCE:dag_id (if exists)
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
        from sqlalchemy import delete

        session.execute(delete(Resource).where(Resource.id.in_(dag_resource_ids)))
        return

    dag_permission_ids = [permission.id for permission in dag_permissions]

    # Delete permission-role associations first (foreign key constraint)
    from sqlalchemy import delete

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
