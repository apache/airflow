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

from airflow.providers.common.compat.sdk import conf

WWW = Path(__file__).resolve().parent
# There is a difference with configuring Swagger in Connexion 2.x and Connexion 3.x
# Connexion 2: https://connexion.readthedocs.io/en/2.14.2/quickstart.html#the-swagger-ui-console
# Connexion 3: https://connexion.readthedocs.io/en/stable/swagger_ui.html#configuring-the-swagger-ui
SWAGGER_ENABLED = conf.getboolean("api", "enable_swagger_ui", fallback=True)
SWAGGER_BUNDLE = WWW.joinpath("static", "dist", "swagger-ui")
