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

from typing import TYPE_CHECKING, Any

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.engine import make_url

from airflow import settings

if TYPE_CHECKING:
    from flask import Flask
    from sqlalchemy.engine import Engine


def _points_at_metadata_db(options: dict[str, Any]) -> bool:
    url = options.get("url")
    conn = settings.SQL_ALCHEMY_CONN
    return url is not None and conn is not None and make_url(url) == make_url(conn)


class AirflowSQLAlchemy(SQLAlchemy):
    """``Flask-SQLAlchemy`` extension bound to Airflow's metadata engine."""

    def _make_engine(self, bind_key: str | None, options: dict[str, Any], app: Flask) -> Engine:
        # A second engine built straight from the connection URI would skip the ``do_connect``
        # handlers that a ``settings.create_metadata_engine`` override installs in
        # ``airflow_local_settings.py`` to mint short-lived credentials per connection. Only the
        # default bind aimed at the metadata database is Airflow's to take over — secondary binds
        # and a ``webserver_config.py`` that points ``SQLALCHEMY_DATABASE_URI`` at another database
        # keep the engine Flask-SQLAlchemy builds for them.
        engine = settings.engine
        if bind_key is None and engine is not None and _points_at_metadata_db(options):
            return engine
        return super()._make_engine(bind_key, options, app)
