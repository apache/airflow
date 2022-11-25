#
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

from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context


class SmoothOperator(BaseOperator):
    """
    Operator that does literally nothing but it logs YouTube link to
    Sade song "Smooth Operator".
    """

    ui_color = "#e8f7e4"
    yt_link: str = "https://www.youtube.com/watch?v=4TYv2PhG89A"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context: Context):
        self.log.info("Enjoy Sade - Smooth Operator: %s", self.yt_link)
