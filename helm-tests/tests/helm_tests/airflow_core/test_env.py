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

import jmespath
import pytest
from chart_utils.helm_template_generator import render_chart


@pytest.mark.parametrize(
    ("airflow_version", "template_yaml"),
    [
        ("2.10.0", "templates/webserver/webserver-deployment.yaml"),
        ("3.0.0", "templates/api-server/api-server-deployment.yaml"),
        ("default", "templates/api-server/api-server-deployment.yaml"),
    ],
)
def test_should_add_airflow_home(airflow_version, template_yaml):
    exp_path = "/not/even/a/real/path"
    values = {"airflowHome": exp_path}

    if airflow_version != "default":
        values["airflowVersion"] = airflow_version

    docs = render_chart(values=values, show_only=[template_yaml])

    assert {"name": "AIRFLOW_HOME", "value": exp_path} in jmespath.search(
        "spec.template.spec.containers[0].env", docs[0]
    )


@pytest.mark.parametrize(
    ("airflow_version", "template_yaml"),
    [
        ("2.10.0", "templates/webserver/webserver-deployment.yaml"),
        ("3.0.0", "templates/api-server/api-server-deployment.yaml"),
        ("default", "templates/api-server/api-server-deployment.yaml"),
    ],
)
def test_should_add_airflow_home_notset(airflow_version, template_yaml):
    values = {}
    if airflow_version != "default":
        values["airflowVersion"] = airflow_version

    docs = render_chart(values=values, show_only=[template_yaml])
    assert {"name": "AIRFLOW_HOME", "value": "/opt/airflow"} in jmespath.search(
        "spec.template.spec.containers[0].env", docs[0]
    )
