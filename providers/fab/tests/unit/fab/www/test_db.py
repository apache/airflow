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

from unittest import mock

import pytest
from flask import Flask
from sqlalchemy import create_engine

from airflow.providers.fab.www.db import AirflowSQLAlchemy


@pytest.fixture
def flask_app():
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite://"
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    return app


@pytest.fixture
def metadata_engine():
    engine = create_engine("sqlite://")
    with (
        mock.patch("airflow.settings.engine", engine),
        mock.patch("airflow.settings.SQL_ALCHEMY_CONN", "sqlite://"),
    ):
        yield engine


class TestAirflowSQLAlchemy:
    def test_binds_to_the_airflow_metadata_engine(self, flask_app, metadata_engine):
        db = AirflowSQLAlchemy(flask_app)

        with flask_app.app_context():
            assert db.engine is metadata_engine

    def test_builds_its_own_engine_for_secondary_binds(self, flask_app, metadata_engine):
        flask_app.config["SQLALCHEMY_BINDS"] = {"other": "sqlite://"}
        db = AirflowSQLAlchemy(flask_app)

        with flask_app.app_context():
            assert db.engines[None] is metadata_engine
            assert db.engines["other"] is not metadata_engine

    def test_builds_its_own_engine_for_another_database(self, flask_app, metadata_engine):
        """``webserver_config.py`` may aim ``SQLALCHEMY_DATABASE_URI`` at a database of its own."""
        flask_app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:////var/lib/airflow/fab.db"
        db = AirflowSQLAlchemy(flask_app)

        with flask_app.app_context():
            assert db.engine is not metadata_engine

    @mock.patch("airflow.settings.engine", None)
    def test_builds_its_own_engine_when_the_orm_is_not_configured(self, flask_app):
        db = AirflowSQLAlchemy(flask_app)

        with flask_app.app_context():
            assert db.engine is not None
            assert str(db.engine.url) == "sqlite://"
