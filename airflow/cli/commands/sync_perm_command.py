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
"""Sync permission command."""
from __future__ import annotations

from airflow.utils import cli as cli_utils


@cli_utils.action_cli
def sync_perm(args):
    """Updates permissions for existing roles and DAGs."""
    from airflow.utils.cli_app_builder import get_application_builder

    with get_application_builder() as appbuilder:
        print("Updating actions and resources for all existing roles")
        # Add missing permissions for all the Base Views _before_ syncing/creating roles
        appbuilder.add_permissions(update_perms=True)
        appbuilder.sm.sync_roles()
        if args.include_dags:
            print("Updating permission on all DAG views")
            appbuilder.sm.create_dag_specific_permissions()
