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

import functools
from unittest import mock


def dont_initialize(f):
    def no_op(*args, **kwargs):
        pass

    @mock.patch("airflow.www.app.init_api_experimental_auth", no_op)
    @mock.patch("airflow.www.app.init_flash_views", no_op)
    @mock.patch("airflow.www.app.init_appbuilder_links", no_op)
    @mock.patch("airflow.www.app.init_appbuilder_views", no_op)
    @mock.patch("airflow.www.app.init_plugins", no_op)
    @mock.patch("airflow.www.app.init_connection_form", no_op)
    @mock.patch("airflow.www.app.init_error_handlers", no_op)
    @mock.patch("airflow.www.app.init_api_connexion", no_op)
    @mock.patch("airflow.www.app.init_api_experimental", no_op)
    @mock.patch("airflow.www.app.sync_appbuilder_roles", no_op)
    @mock.patch("airflow.www.app.init_jinja_globals", no_op)
    @mock.patch("airflow.www.app.init_xframe_protection", no_op)
    @mock.patch("airflow.www.app.init_permanent_session", no_op)
    @mock.patch("airflow.www.app.init_appbuilder", no_op)
    @functools.wraps(f)
    def func(*args, **kwargs):
        return f(*args, **kwargs)

    return func
