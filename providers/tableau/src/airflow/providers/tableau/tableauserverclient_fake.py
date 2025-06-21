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


class AuthMock:
    """Fake Auth class required in the Server class below."""

    def sign_in(self, auth):
        return True

    def sign_out(self):
        return True


class JobsMock:
    """Fake Jobs class required in the Server class below."""

    def get_by_id(self, job_id):
        return {"id": job_id, "status": "completed"}


class Server:
    """Fake Tableau Server class to pass CI with python 3.10."""

    def __init__(self, host):
        self.host = host
        self.use_server_version_called = False
        self.http_options = None
        self.session_factory = None
        self.auth = AuthMock()
        self.jobs = JobsMock()

    def add_http_options(self, *args, **kwargs):
        pass

    def use_server_version(self):
        self.use_server_version_called = True


class TableauAuth:
    """Fake TableauAuth class to pass CI with python 3.10."""

    def __init__(self, username: str, password: str, site_id: str):
        pass


class JWTAuth:
    """Fake Tableau JWTAuth class to pass CI with python 3.10."""

    def __init__(self, jwt_token: str, site_id: str):
        pass


class Pager:
    """Fake Tableau Pager class to pass CI with python 3.10."""

    def __init__(self, request_opts, **kwargs):
        pass
