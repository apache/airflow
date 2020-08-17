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

"""Role based authentication backend - access is based on user roles."""
from functools import wraps
from typing import Callable, TypeVar

from flask import Response, current_app

CLIENT_AUTH = None


def init_app(_):
    """Initializes authentication backend"""


T = TypeVar("T", bound=Callable)  # pylint: disable=invalid-name


def requires_authentication(function):
    """Decorator for functions that require authentication"""

    @wraps(function)
    def decorated(*args, **kwargs):
        appbuilder = current_app.appbuilder
        view_permissions = {
            "trigger_dag": [("can_trigger", "Airflow")],
            "delete_dag": [("can_delete", "Airflow")],
            "dag_paused": [("can_paused", "Airflow")],
            "create_pool": [("can_add", "PoolModelView")],
            "delete_pool": [("can_delete", "PoolModelView")],
        }

        dag_permissions = {
            "delete_dag": [
                ("can_dag_edit", "all_dags"),
                ("can_dag_edit", kwargs.get("dag_id")),
            ],
            "dag_paused": [
                ("can_dag_edit", "all_dags"),
                ("can_dag_edit", kwargs.get("dag_id")),
            ],
        }

        func_name = function.__name__
        for permission in view_permissions.get(func_name, []):
            if not appbuilder.sm.has_access(*permission):
                return Response("Forbidden", 403)

        if func_name in dag_permissions:
            if not any(
                appbuilder.sm.has_access(*permission)
                for permission in dag_permissions[func_name]
            ):
                return Response("Forbidden", 403)

        return function(*args, **kwargs)

    return decorated
