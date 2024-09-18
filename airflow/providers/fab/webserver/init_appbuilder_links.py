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

from airflow.configuration import conf
from airflow.security.permissions import RESOURCE_DOCS, RESOURCE_DOCS_MENU
from airflow.utils.docs import get_docs_url


def init_appbuilder_links(app):
    """Add links to the navbar."""
    appbuilder = app.appbuilder

    appbuilder.add_link(name="DAGs", href="Airflow.index")
    appbuilder.menu.menu.insert(0, appbuilder.menu.menu.pop())  # Place in the first menu slot
    appbuilder.add_link(name="Cluster Activity", href="Airflow.cluster_activity")
    appbuilder.menu.menu.insert(1, appbuilder.menu.menu.pop())  # Place in the second menu slot
    appbuilder.add_link(name="Datasets", href="Airflow.datasets")
    appbuilder.menu.menu.insert(2, appbuilder.menu.menu.pop())  # Place in the third menu slot

    # Docs links
    appbuilder.add_link(
        name=RESOURCE_DOCS, label="Documentation", href=get_docs_url(), category=RESOURCE_DOCS_MENU
    )
    appbuilder.add_link(
        name=RESOURCE_DOCS,
        label="Airflow Website",
        href="https://airflow.apache.org",
        category=RESOURCE_DOCS_MENU,
    )
    appbuilder.add_link(
        name=RESOURCE_DOCS,
        label="GitHub Repo",
        href="https://github.com/apache/airflow",
        category=RESOURCE_DOCS_MENU,
    )

    if conf.getboolean("webserver", "enable_swagger_ui", fallback=True):
        appbuilder.add_link(
            name=RESOURCE_DOCS,
            label="REST API Reference (Swagger UI)",
            href="/api/v1./api/v1_swagger_ui_index",
            category=RESOURCE_DOCS_MENU,
        )
    appbuilder.add_link(
        name=RESOURCE_DOCS,
        label="REST API Reference (Redoc)",
        href="RedocView.redoc",
        category=RESOURCE_DOCS_MENU,
    )
