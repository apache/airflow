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

import os

import pytest

import airflow
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.providers.fab.auth_manager.cli_commands.utils import get_application_builder
from airflow.providers.fab.www.extensions.init_appbuilder import AirflowAppBuilder
from airflow.providers.fab.www.session import AirflowDatabaseSessionInterface

from tests_common.test_utils.config import conf_vars


@pytest.fixture
def flask_app():
    """Fixture to set up the Flask app with the necessary configuration."""
    # Get the webserver config file path
    webserver_config = conf.get_mandatory_value("fab", "config_file")
    with get_application_builder() as appbuilder:
        flask_app = appbuilder.app

        # Load webserver configuration
        flask_app.config.from_pyfile(webserver_config, silent=True)
        flask_app.config.from_prefixed_env(prefix="AIRFLOW__FAB__CONFIG__")

        yield flask_app


@pytest.mark.db_test
class TestCliUtils:
    def test_get_application_builder(self):
        """Test that get_application_builder returns an AirflowAppBuilder instance."""
        with get_application_builder() as appbuilder:
            assert isinstance(appbuilder, AirflowAppBuilder)

    def test_sqlalchemy_uri_configured(self, flask_app):
        """Test that the SQLALCHEMY_DATABASE_URI is correctly set in the Flask app."""
        sqlalchemy_uri = conf.get("database", "SQL_ALCHEMY_CONN")

        # Assert that the SQLAlchemy URI is correctly set
        assert sqlalchemy_uri == flask_app.config["SQLALCHEMY_DATABASE_URI"]

    def test_relative_path_sqlite_raises_exception(self):
        """Test that a relative SQLite path raises an AirflowConfigException."""
        # Directly simulate the configuration for relative SQLite path
        with conf_vars({("database", "SQL_ALCHEMY_CONN"): "sqlite://relative/path"}):
            with pytest.raises(AirflowConfigException, match="Cannot use relative path"):
                with get_application_builder():
                    pass

    def test_static_folder_exists(self, flask_app):
        """Test that the static folder is correctly configured in the Flask app."""
        static_folder = os.path.join(os.path.dirname(airflow.__file__), "www", "static")
        assert flask_app.static_folder == static_folder

    def test_database_auth_backend_in_session(self, flask_app):
        """Test that the database is used for session management when AUTH_BACKEND is set to 'database'."""

        with get_application_builder() as appbuilder:
            flask_app = appbuilder.app
            # Ensure that the correct session interface is set (for 'database' auth backend)
            assert isinstance(flask_app.session_interface, AirflowDatabaseSessionInterface)
