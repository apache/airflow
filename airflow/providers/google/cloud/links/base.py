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

from typing import TYPE_CHECKING, ClassVar

from airflow.models import BaseOperatorLink, XCom

if TYPE_CHECKING:
    from airflow.models import BaseOperator
    from airflow.models.taskinstance import TaskInstanceKey


BASE_LINK = "https://console.cloud.google.com"


class BaseGoogleLink(BaseOperatorLink):
    """:meta private:"""

    name: ClassVar[str]
    key: ClassVar[str]
    format_str: ClassVar[str]

    def get_link(
        self,
        operator: BaseOperator,
        *,
        ti_key: TaskInstanceKey,
    ) -> str:
        conf = XCom.get_value(key=self.key, ti_key=ti_key)
        if not conf:
            return ""
        if self.format_str.startswith("http"):
            return self.format_str.format(**conf)
        return BASE_LINK + self.format_str.format(**conf)
