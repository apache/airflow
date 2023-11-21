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

from airflow.auth.managers.fab.security_manager.constants import EXISTING_ROLES as FAB_EXISTING_ROLES
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
    ACTION_CAN_ACCESS_MENU,
    ACTION_CAN_READ,
    RESOURCE_ADMIN_MENU,
    RESOURCE_AUDIT_LOG,
    RESOURCE_BROWSE_MENU,
    RESOURCE_CLUSTER_ACTIVITY,
    RESOURCE_CONFIG,
    RESOURCE_CONNECTION,
    RESOURCE_DAG,
    RESOURCE_DAG_CODE,
    RESOURCE_DAG_DEPENDENCIES,
    RESOURCE_DAG_RUN,
    RESOURCE_DATASET,
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
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.www.extensions.init_auth_manager import get_auth_manager
from airflow.www.utils import CustomSQLAInterface

EXISTING_ROLES = FAB_EXISTING_ROLES

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.auth.managers.fab.models import Action, Resource
    from airflow.auth.managers.models.base_user import BaseUser


class AirflowSecurityManagerV2(LoggingMixin):
    """Custom security manager, which introduces a permission model adapted to Airflow.

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
        """Perform the required steps when initializing airflow for standalone mode.

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
    @provide_session
    def _auth_manager_is_authorized_map(
        self, session: Session = NEW_SESSION
    ) -> dict[str, Callable[[str, str | None, BaseUser | None], bool]]:
        """
        Return the map associating a FAB resource name to the corresponding auth manager is_authorized_ API.

        The function returned takes the FAB action name and the user as parameter.
        """
        auth_manager = get_auth_manager()
        methods = get_method_from_fab_action_map()

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
            ti = session.scalar(
                select(DagRun)
                .where(
                    TaskInstance.dag_id == composite_pk[0],
                    TaskInstance.task_id == composite_pk[1],
                    TaskInstance.run_id == composite_pk[2],
                    TaskInstance.map_index >= composite_pk[3],
                )
                .limit(1)
            )
            if not ti:
                raise AirflowException("Task instance not found")
            return ti.dag_id

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
                raise AirflowException("Connection not found")
            return variable.key

        return {
            RESOURCE_AUDIT_LOG: lambda action, resource_pk, user: auth_manager.is_authorized_dag(
                method=methods[action],
                access_entity=DagAccessEntity.AUDIT_LOG,
                user=user,
            ),
            RESOURCE_CLUSTER_ACTIVITY: lambda action, resource_pk, user: auth_manager.is_authorized_view(
                access_view=AccessView.CLUSTER_ACTIVITY,
                user=user,
            ),
            RESOURCE_CONFIG: lambda action, resource_pk, user: auth_manager.is_authorized_configuration(
                method=methods[action],
                user=user,
            ),
            RESOURCE_CONNECTION: lambda action, resource_pk, user: auth_manager.is_authorized_connection(
                method=methods[action],
                details=ConnectionDetails(conn_id=get_connection_id(resource_pk)),
                user=user,
            ),
            RESOURCE_DAG: lambda action, resource_pk, user: auth_manager.is_authorized_dag(
                method=methods[action],
                user=user,
            ),
            RESOURCE_DAG_CODE: lambda action, resource_pk, user: auth_manager.is_authorized_dag(
                method=methods[action],
                access_entity=DagAccessEntity.CODE,
                user=user,
            ),
            RESOURCE_DAG_DEPENDENCIES: lambda action, resource_pk, user: auth_manager.is_authorized_dag(
                method=methods[action],
                access_entity=DagAccessEntity.DEPENDENCIES,
                user=user,
            ),
            RESOURCE_DAG_RUN: lambda action, resource_pk, user: auth_manager.is_authorized_dag(
                method=methods[action],
                access_entity=DagAccessEntity.RUN,
                details=DagDetails(id=get_dag_id_from_dagrun_id(resource_pk)),
                user=user,
            ),
            RESOURCE_DATASET: lambda action, resource_pk, user: auth_manager.is_authorized_dataset(
                method=methods[action],
                user=user,
            ),
            RESOURCE_DOCS: lambda action, resource_pk, user: auth_manager.is_authorized_view(
                access_view=AccessView.DOCS,
                user=user,
            ),
            RESOURCE_PLUGIN: lambda action, resource_pk, user: auth_manager.is_authorized_view(
                access_view=AccessView.PLUGINS,
                user=user,
            ),
            RESOURCE_JOB: lambda action, resource_pk, user: auth_manager.is_authorized_view(
                access_view=AccessView.JOBS,
                user=user,
            ),
            RESOURCE_POOL: lambda action, resource_pk, user: auth_manager.is_authorized_pool(
                method=methods[action],
                details=PoolDetails(name=get_pool_name(resource_pk)),
                user=user,
            ),
            RESOURCE_PROVIDER: lambda action, resource_pk, user: auth_manager.is_authorized_view(
                access_view=AccessView.PROVIDERS,
                user=user,
            ),
            RESOURCE_SLA_MISS: lambda action, resource_pk, user: auth_manager.is_authorized_dag(
                method=methods[action],
                access_entity=DagAccessEntity.SLA_MISS,
                user=user,
            ),
            RESOURCE_TASK_INSTANCE: lambda action, resource_pk, user: auth_manager.is_authorized_dag(
                method=methods[action],
                access_entity=DagAccessEntity.TASK_INSTANCE,
                details=DagDetails(id=get_dag_id_from_task_instance(resource_pk)),
                user=user,
            ),
            RESOURCE_TASK_RESCHEDULE: lambda action, resource_pk, user: auth_manager.is_authorized_dag(
                method=methods[action],
                access_entity=DagAccessEntity.TASK_RESCHEDULE,
                user=user,
            ),
            RESOURCE_TRIGGER: lambda action, resource_pk, user: auth_manager.is_authorized_view(
                access_view=AccessView.TRIGGERS,
                user=user,
            ),
            RESOURCE_VARIABLE: lambda action, resource_pk, user: auth_manager.is_authorized_variable(
                method=methods[action],
                details=VariableDetails(key=get_variable_key(resource_pk)),
                user=user,
            ),
            RESOURCE_XCOM: lambda action, resource_pk, user: auth_manager.is_authorized_dag(
                method=methods[action],
                access_entity=DagAccessEntity.XCOM,
                user=user,
            ),
        }

    def _get_auth_manager_is_authorized_method(self, fab_resource_name: str) -> Callable:
        is_authorized_method = self._auth_manager_is_authorized_map.get(fab_resource_name)
        if is_authorized_method:
            return is_authorized_method
        elif fab_resource_name in [RESOURCE_DOCS_MENU, RESOURCE_ADMIN_MENU, RESOURCE_BROWSE_MENU]:
            # Display the "Browse", "Admin" and "Docs" dropdowns in the menu if the user has access to at
            # least one dropdown child
            return self._is_authorized_category_menu(fab_resource_name)
        else:
            # This means the page the user is trying to access is specific to the auth manager used
            # Example: the user list view in FabAuthManager
            return lambda action, resource_pk, user: get_auth_manager().is_authorized_custom_view(
                fab_action_name=ACTION_CAN_READ if action == ACTION_CAN_ACCESS_MENU else action,
                fab_resource_name=fab_resource_name,
                user=user,
            )

    def _is_authorized_category_menu(self, category: str) -> Callable:
        items = {item.name for item in self.appbuilder.menu.find(category).childs}
        return lambda action, resource_pk, user: any(
            self._get_auth_manager_is_authorized_method(fab_resource_name=item)(action, resource_pk, user)
            for item in items
        )

    """
    The following methods are specific to FAB auth manager. They still need to be "present" in the main
    security manager class, but they do nothing.
    """

    def get_action(self, name: str) -> Action:
        raise NotImplementedError("Only available if FAB auth manager is used")

    def get_resource(self, name: str) -> Resource:
        raise NotImplementedError("Only available if FAB auth manager is used")

    def add_permissions_view(self, base_action_names, resource_name):
        pass

    def add_permissions_menu(self, resource_name):
        pass
