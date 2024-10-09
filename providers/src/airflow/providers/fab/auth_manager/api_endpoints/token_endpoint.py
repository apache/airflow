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

from functools import wraps
from typing import TYPE_CHECKING, Callable, TypeVar, cast

from airflow.api_connexion.security import check_authentication
from airflow.configuration import conf
from airflow.providers.fab.auth_manager.schemas.token_schema import token_schema
from airflow.utils.jwt_signer import JWTSigner
from airflow.www.extensions.init_auth_manager import get_auth_manager

if TYPE_CHECKING:
    from airflow.api_connexion.types import APIResponse

T = TypeVar("T", bound=Callable)


def requires_authentication() -> Callable[[T], T]:
    def requires_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            check_authentication()
            return func(*args, **kwargs)

        return cast(T, decorated)

    return requires_access_decorator


@requires_authentication()
def get_token() -> APIResponse:
    user = get_auth_manager().get_user()

    signer = JWTSigner(
        secret_key=conf.get("api", "auth_jwt_secret"),
        expiration_time_in_seconds=conf.getint("api", "auth_jwt_expiration_time"),
        audience="apis",
    )
    token = signer.generate_signed_token(get_auth_manager().serialize_user(user))

    return token_schema.dump({"access_token": token, "token_type": "bearer"})
