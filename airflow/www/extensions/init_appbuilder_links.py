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

from airflow import version


def init_appbuilder_links(app):
    """Add links to Docs menu in navbar"""
    appbuilder = app.appbuilder
    if "dev" in version.version:
        doc_site = "https://airflow.readthedocs.io/en/latest"
    else:
        doc_site = 'https://airflow.apache.org/docs/{}'.format(version.version)

    appbuilder.add_link(
        "Website", href='https://airflow.apache.org', category="Docs", category_icon="fa-globe"
    )
    appbuilder.add_link("Documentation", href=doc_site, category="Docs", category_icon="fa-cube")
    appbuilder.add_link("GitHub", href='https://github.com/apache/airflow', category="Docs")
    appbuilder.add_link("REST API Reference", href='/api/v1./api/v1_swagger_ui_index', category="Docs")
