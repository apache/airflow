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

from airflow.models.baseoperator import BaseOperatorLink
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.http.operators.http import HttpOperator


# define the extra link
class HTTPDocsLink(BaseOperatorLink):
    # name the link button in the UI
    name = "HTTP docs"

    # add the button to one or more operators
    operators = [HttpOperator]

    # provide the link
    def get_link(self, operator, *, ti_key=None):
        return "https://developer.mozilla.org/en-US/docs/Web/HTTP"


# define the plugin class
class AirflowExtraLinkPlugin(AirflowPlugin):
    name = "extra_link_plugin"
    operator_extra_links = [
        HTTPDocsLink(),
    ]
