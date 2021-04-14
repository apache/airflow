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

from flask import Response, current_app
from flask_jwt_extended import verify_jwt_in_request

from airflow.api_connexion.exceptions import PermissionDenied, Unauthenticated
from airflow.models import TokenBlockList

T = TypeVar("T", bound=Callable)  # pylint: disable=invalid-name


@current_app.appbuilder.sm.jwt_manager.token_in_blacklist_loader
def check_if_token_in_blacklist(decrypted_token):
    """Checks if there's a blocked token"""
    jti = decrypted_token['jti']
    return TokenBlockList.get_token(jti) is not None


def check_authentication() -> None:
    """Checks that the request has valid authorization information."""
    response = current_app.api_auth.requires_authentication(Response)()
    if response.status_code == 200:
        return
    try:
        verify_jwt_in_request()
        return
    except Exception:  # pylint: disable=broad-except
        pass
        # since this handler only checks authentication, not authorization,
        # we should always return 401
    raise Unauthenticated(headers=response.headers)


def requires_access(permissions: Optional[Sequence[Tuple[str, str]]] = None) -> Callable[[T], T]:
    """Factory for decorator that checks current user's permissions against required permissions."""
    appbuilder = current_app.appbuilder
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
