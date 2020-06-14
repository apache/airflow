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

from airflow.configuration import conf


def init_xframe_protection(app):
    @app.after_request
    def apply_caching(response):
        _x_frame_enabled = conf.getboolean('webserver', 'X_FRAME_ENABLED', fallback=True)
        if not _x_frame_enabled:
            response.headers["X-Frame-Options"] = "DENY"
        return response


def init_api_experimental_auth(app):
    from airflow import api
    api.load_auth()
    api.API_AUTH.api_auth.init_app(app)
