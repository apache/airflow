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

from tests.charts.helm_template_generator import render_chart


def test_should_add_airflow_home():
    exp_path = "/not/even/a/real/path"
    docs = render_chart(
        values={"airflowHome": exp_path},
        show_only=["templates/webserver/webserver-deployment.yaml"],
    )
    assert {"name": "AIRFLOW_HOME", "value": exp_path} in jmespath.search(
        "spec.template.spec.containers[0].env", docs[0]
    )


def test_should_add_airflow_home_notset():
    docs = render_chart(
        values={},
        show_only=["templates/webserver/webserver-deployment.yaml"],
    )
    assert {"name": "AIRFLOW_HOME", "value": "/opt/airflow"} in jmespath.search(
        "spec.template.spec.containers[0].env", docs[0]
    )
