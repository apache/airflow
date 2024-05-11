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

from pathlib import Path

from flask import Blueprint

from airflow.plugins_manager import AirflowPlugin

PLUGIN_NAME = Path(__file__).stem


class GridPanelExample(AirflowPlugin):
    name = PLUGIN_NAME
    flask_blueprints = [
        Blueprint(
            PLUGIN_NAME,
            __name__,
            static_folder=f"{PLUGIN_NAME}_static",
            static_url_path=f"/{PLUGIN_NAME}",  # This is an example where code & resources can be served from
        )
    ]
