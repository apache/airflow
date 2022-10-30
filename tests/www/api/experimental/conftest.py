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
from __future__ import annotations

import pytest

from airflow.www import app as application
from tests.test_utils.config import conf_vars
from tests.test_utils.decorators import dont_initialize_flask_app_submodules

# This entire directory is testing deprecated functions.
pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


@pytest.fixture(scope="session")
def experiemental_api_app():
    @conf_vars({("api", "enable_experimental_api"): "true"})
    @dont_initialize_flask_app_submodules(
        skip_all_except=[
            "init_api_experimental_auth",
            "init_appbuilder_views",
            "init_api_experimental",
            "init_appbuilder",
        ]
    )
    def factory():
        app = application.create_app(testing=True)
        app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///"
        app.config["SECRET_KEY"] = "secret_key"
        app.config["CSRF_ENABLED"] = False
        app.config["WTF_CSRF_ENABLED"] = False
        return app

    return factory()
