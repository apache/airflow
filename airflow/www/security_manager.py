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

import json
from functools import cached_property
from typing import TYPE_CHECKING, Callable

from flask import g
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from sqlalchemy import select

from airflow.auth.managers.models.resource_details import (
    AccessView,
    ConnectionDetails,
    DagAccessEntity,
    DagDetails,
    PoolDetails,
    VariableDetails,
)
from airflow.auth.managers.utils.fab import (
    get_method_from_fab_action_map,
)
from airflow.exceptions import AirflowException
from airflow.models import Connection, DagRun, Pool, TaskInstance, Variable
from airflow.security.permissions import (
    RESOURCE_ADMIN_MENU,
    RESOURCE_ASSET,
    RESOURCE_AUDIT_LOG,
    RESOURCE_BROWSE_MENU,
    RESOURCE_CLUSTER_ACTIVITY,
    RESOURCE_CONFIG,
    RESOURCE_CONNECTION,
    RESOURCE_DAG,
    RESOURCE_DAG_CODE,
    RESOURCE_DAG_DEPENDENCIES,
    RESOURCE_DAG_RUN,
    RESOURCE_DOCS,
    RESOURCE_DOCS_MENU,
    RESOURCE_JOB,
    RESOURCE_PLUGIN,
    RESOURCE_POOL,
    RESOURCE_PROVIDER,
    RESOURCE_SLA_MISS,
    RESOURCE_TASK_INSTANCE,
    RESOURCE_TASK_RESCHEDULE,
    RESOURCE_TRIGGER,
    RESOURCE_VARIABLE,
    RESOURCE_XCOM,
)
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.www.extensions.init_auth_manager import get_auth_manager
from airflow.www.utils import CustomSQLAInterface

EXISTING_ROLES = {
    "Admin",
    "Viewer",
    "User",
    "Op",
    "Public",
}

if TYPE_CHECKING:
    from airflow.auth.managers.models.base_user import BaseUser


class AirflowSecurityManagerV2(LoggingMixin):
    """
    Custom security manager, which introduces a permission model adapted to Airflow.

    It's named V2 to differentiate it from the obsolete airflow.www.security.AirflowSecurityManager.
    """

    def __init__(self, appbuilder) -> None:
        super().__init__()
        self.appbuilder = appbuilder

        # Setup Flask-Limiter
        self.limiter = self.create_limiter()

        # Go and fix up the SQLAInterface used from the stock one to our subclass.
        # This is needed to support the "hack" where we had to edit
        # FieldConverter.conversion_table in place in airflow.www.utils
        for attr in dir(self):
            if attr.endswith("view"):
                view = getattr(self, attr, None)
                if view and getattr(view, "datamodel", None):
                    view.datamodel = CustomSQLAInterface(view.datamodel.obj)

    @staticmethod
    def before_request():
        """Run hook before request."""
        g.user = get_auth_manager().get_user()

    def create_limiter(self) -> Limiter:
        limiter = Limiter(key_func=get_remote_address)
        limiter.init_app(self.appbuilder.get_app)
        return limiter

    def register_views(self):
        """Allow auth managers to register their own views. By default, do nothing."""
        pass

    def has_access(
        self, action_name: str, resource_name: str, user=None, resource_pk: str | None = None
    ) -> bool:
        """
        Verify whether a given user could perform a certain action on the given resource.

        Example actions might include can_read, can_write, can_delete, etc.

        This function is called by FAB when accessing a view. See
        https://github.com/dpgaspar/Flask-AppBuilder/blob/c6fecdc551629e15467fde5d06b4437379d90592/flask_appbuilder/security/decorators.py#L134

        :param action_name: action_name on resource (e.g can_read, can_edit).
        :param resource_name: name of view-menu or resource.
        :param user: user
        :param resource_pk: the resource primary key (e.g. the connection ID)
        :return: Whether user could perform certain action on the resource.
        """
        if not user:
            user = g.user

        is_authorized_method = self._get_auth_manager_is_authorized_method(resource_name)
        return is_authorized_method(action_name, resource_pk, user)

    def create_admin_standalone(self) -> tuple[str | None, str | None]:
        """
        Perform the required steps when initializing airflow for standalone mode.

        If necessary, returns the username and password to be printed in the console for users to log in.
        """
        return None, None

    def add_limit_view(self, baseview):
        if not baseview.limits:
            return

        for limit in baseview.limits:
            self.limiter.limit(
                limit_value=limit.limit_value,
                key_func=limit.key_func,
                per_method=limit.per_method,
                methods=limit.methods,
                error_message=limit.error_message,
                exempt_when=limit.exempt_when,
                override_defaults=limit.override_defaults,
                deduct_when=limit.deduct_when,
                on_breach=limit.on_breach,
                cost=limit.cost,
            )(baseview.blueprint)

    @cached_property
    def _auth_manager_is_authorized_map(
        self,
    ) -> dict[str, Callable[[str, str | None, BaseUser | None], bool]]:
        """
        Return the map associating a FAB resource name to the corresponding auth manager is_authorized_ API.

        The function returned takes the FAB action name and the user as parameter.
        """
        auth_manager = get_auth_manager()
        methods = get_method_from_fab_action_map()

        session = self.appbuilder.session

        def get_connection_id(resource_pk):
            if not resource_pk:
                return None
            connection = session.scalar(select(Connection).where(Connection.id == resource_pk).limit(1))
            if not connection:
                raise AirflowException("Connection not found")
            return connection.conn_id

        def get_dag_id_from_dagrun_id(resource_pk):
            if not resource_pk:
                return None
            dagrun = session.scalar(select(DagRun).where(DagRun.id == resource_pk).limit(1))
            if not dagrun:
                raise AirflowException("DagRun not found")
            return dagrun.dag_id

        def get_dag_id_from_task_instance(resource_pk):
            if not resource_pk:
                return None
            composite_pk = json.loads(resource_pk)
            dag_id = session.scalar(
                select(TaskInstance.dag_id)
                .where(
                    TaskInstance.dag_id == composite_pk[0],
                    TaskInstance.task_id == composite_pk[1],
                    TaskInstance.run_id == composite_pk[2],
                    TaskInstance.map_index >= composite_pk[3],
                )
                .limit(1)
            )
            if not dag_id:
                raise AirflowException("Task instance not found")
            return dag_id

        def get_pool_name(resource_pk):
            if not resource_pk:
                return None
            pool = session.scalar(select(Pool).where(Pool.id == resource_pk).limit(1))
            if not pool:
                raise AirflowException("Pool not found")
            return pool.pool

        def get_variable_key(resource_pk):
            if not resource_pk:
                return None
            variable = session.scalar(select(Variable).where(Variable.id == resource_pk).limit(1))
            if not variable:
                raise AirflowException("Variable not found")
            return variable.key

        def _is_authorized_view(view_):
            return lambda action, resource_pk, user: auth_manager.is_authorized_view(
                access_view=view_,
                user=user,
            )

        def _is_authorized_dag(entity_=None, details_func_=None):
            return lambda action, resource_pk, user: auth_manager.is_authorized_dag(
                method=methods[action],
                access_entity=entity_,
                details=DagDetails(id=details_func_(resource_pk)) if details_func_ else None,
                user=user,
            )

        mapping = {
            RESOURCE_CONFIG: lambda action, resource_pk, user: auth_manager.is_authorized_configuration(
                method=methods[action],
                user=user,
            ),
            RESOURCE_CONNECTION: lambda action, resource_pk, user: auth_manager.is_authorized_connection(
                method=methods[action],
                details=ConnectionDetails(conn_id=get_connection_id(resource_pk)),
                user=user,
            ),
            RESOURCE_ASSET: lambda action, resource_pk, user: auth_manager.is_authorized_dataset(
                method=methods[action],
                user=user,
            ),
            RESOURCE_POOL: lambda action, resource_pk, user: auth_manager.is_authorized_pool(
                method=methods[action],
                details=PoolDetails(name=get_pool_name(resource_pk)),
                user=user,
            ),
            RESOURCE_VARIABLE: lambda action, resource_pk, user: auth_manager.is_authorized_variable(
                method=methods[action],
                details=VariableDetails(key=get_variable_key(resource_pk)),
                user=user,
            ),
        }
        for resource, entity, details_func in [
            (RESOURCE_DAG, None, None),
            (RESOURCE_AUDIT_LOG, DagAccessEntity.AUDIT_LOG, None),
            (RESOURCE_DAG_CODE, DagAccessEntity.CODE, None),
            (RESOURCE_DAG_DEPENDENCIES, DagAccessEntity.DEPENDENCIES, None),
            (RESOURCE_SLA_MISS, DagAccessEntity.SLA_MISS, None),
            (RESOURCE_TASK_RESCHEDULE, DagAccessEntity.TASK_RESCHEDULE, None),
            (RESOURCE_XCOM, DagAccessEntity.XCOM, None),
            (RESOURCE_DAG_RUN, DagAccessEntity.RUN, get_dag_id_from_dagrun_id),
            (RESOURCE_TASK_INSTANCE, DagAccessEntity.TASK_INSTANCE, get_dag_id_from_task_instance),
        ]:
            mapping[resource] = _is_authorized_dag(entity, details_func)
        for resource, view in [
            (RESOURCE_CLUSTER_ACTIVITY, AccessView.CLUSTER_ACTIVITY),
            (RESOURCE_DOCS, AccessView.DOCS),
            (RESOURCE_PLUGIN, AccessView.PLUGINS),
            (RESOURCE_JOB, AccessView.JOBS),
            (RESOURCE_PROVIDER, AccessView.PROVIDERS),
            (RESOURCE_TRIGGER, AccessView.TRIGGERS),
        ]:
            mapping[resource] = _is_authorized_view(view)
        return mapping

    def _get_auth_manager_is_authorized_method(self, fab_resource_name: str) -> Callable:
        is_authorized_method = self._auth_manager_is_authorized_map.get(fab_resource_name)
        if is_authorized_method:
            return is_authorized_method
        elif fab_resource_name in [RESOURCE_DOCS_MENU, RESOURCE_ADMIN_MENU, RESOURCE_BROWSE_MENU]:
            # Display the "Browse", "Admin" and "Docs" dropdowns in the menu if the user has access to at
            # least one dropdown child
            return self._is_authorized_category_menu(fab_resource_name)
        else:
            # The user is trying to access a page specific to the auth manager
            # (e.g. the user list view in FabAuthManager) or a page defined in a plugin
            return lambda action, resource_pk, user: get_auth_manager().is_authorized_custom_view(
                method=get_method_from_fab_action_map().get(action, action),
                resource_name=fab_resource_name,
                user=user,
            )

    def _is_authorized_category_menu(self, category: str) -> Callable:
        items = {item.name for item in self.appbuilder.menu.find(category).childs}
        return lambda action, resource_pk, user: any(
            self._get_auth_manager_is_authorized_method(fab_resource_name=item)(action, resource_pk, user)
            for item in items
        )

    def add_permissions_view(self, base_action_names, resource_name):
        pass

    def add_permissions_menu(self, resource_name):
        pass
