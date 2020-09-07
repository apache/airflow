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
from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP


def create_user(app, username, role):
    appbuilder = app.appbuilder
    role_admin = appbuilder.sm.find_role(role)
    tester = appbuilder.sm.find_user(username=username)
    if not tester:
        appbuilder.sm.add_user(
            username=username,
            first_name=username,
            last_name=username,
            email=f"{username}@fab.org",
            role=role_admin,
            password=username,
        )


def delete_user(app, username):
    appbuilder = app.appbuilder
    user = next(u for u in appbuilder.sm.get_all_users() if u.username == username)

    appbuilder.sm.del_register_user(user)


def assert_401(response):
    assert response.status_code == 401
    assert response.json == {
        'detail': None,
        'status': 401,
        'title': 'Unauthorized',
        'type': EXCEPTIONS_LINK_MAP[401]
    }
