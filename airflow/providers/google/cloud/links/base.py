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
from datetime import datetime
from typing import TYPE_CHECKING, ClassVar, Optional

from airflow.models import BaseOperatorLink, XCom

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstanceKey


class BaseGoogleLink(BaseOperatorLink):
    """:meta private:"""

    name: ClassVar[str]
    key: ClassVar[str]
    format_str: ClassVar[str]

    def get_link(
        self,
        operator,
        dttm: Optional[datetime] = None,
        ti_key: Optional["TaskInstanceKey"] = None,
    ) -> str:
        if ti_key is not None:
            conf = XCom.get_value(key=self.key, ti_key=ti_key)
        else:
            assert dttm
            conf = XCom.get_one(
                key=self.key,
                dag_id=operator.dag.dag_id,
                task_id=operator.task_id,
                execution_date=dttm,
            )

        return self.format_str.format(**conf) if conf else ""
