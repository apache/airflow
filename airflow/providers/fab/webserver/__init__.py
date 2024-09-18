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

from airflow.providers.fab.webserver.init_appbuilder import init_appbuilder
from airflow.providers.fab.webserver.init_appbuilder_links import init_appbuilder_links
from airflow.providers.fab.webserver.init_views import init_appbuilder_views


def init_fab(app):
    init_appbuilder(app)
    init_appbuilder_links(app)
    init_appbuilder_views(app)
