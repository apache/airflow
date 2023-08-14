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
from contextlib import contextmanager
from functools import lru_cache
from typing import Generator

from flask import Flask

import airflow
from airflow.www.extensions.init_appbuilder import AirflowAppBuilder, init_appbuilder
from airflow.www.extensions.init_views import init_plugins


@lru_cache(maxsize=None)
def _return_appbuilder(app: Flask) -> AirflowAppBuilder:
    """Return an appbuilder instance for the given app."""
    init_appbuilder(app)
    init_plugins(app)
    return app.appbuilder  # type: ignore[attr-defined]


@contextmanager
def get_application_builder() -> Generator[AirflowAppBuilder, None, None]:
    static_folder = os.path.join(os.path.dirname(airflow.__file__), "www", "static")
    flask_app = Flask(__name__, static_folder=static_folder)
    with flask_app.app_context():
        yield _return_appbuilder(flask_app)
