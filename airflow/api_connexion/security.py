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

from functools import wraps
from typing import Callable, Optional, Sequence, Tuple, TypeVar, cast

from flask import Response

from airflow.api_connexion.exceptions import PermissionDenied, Unauthenticated
from airflow.utils.airflow_flask_app import get_airflow_app

T = TypeVar("T", bound=Callable)


def check_authentication() -> None:
    """Checks that the request has valid authorization information."""
    for auth in get_airflow_app().api_auth:
        response = auth.requires_authentication(Response)()
        if response.status_code == 200:
            return
    # since this handler only checks authentication, not authorization,
    # we should always return 401
    raise Unauthenticated(headers=response.headers)


def requires_access(permissions: Optional[Sequence[Tuple[str, str]]] = None) -> Callable[[T], T]:
    """Factory for decorator that checks current user's permissions against required permissions."""
    appbuilder = get_airflow_app().appbuilder
    appbuilder.sm.sync_resource_permissions(permissions)

    def requires_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            check_authentication()
            if appbuilder.sm.check_authorization(permissions, kwargs.get('dag_id')):
                return func(*args, **kwargs)
            raise PermissionDenied()

        return cast(T, decorated)

    return requires_access_decorator
