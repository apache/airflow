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

from datetime import datetime
from typing import TYPE_CHECKING, ClassVar

from airflow.models import BaseOperatorLink, XCom

if TYPE_CHECKING:
    from airflow.models import BaseOperator
    from airflow.models.taskinstance import TaskInstanceKey
    from airflow.utils.context import Context


BASE_AWS_CONSOLE_LINK = "https://console.{aws_domain}"


class BaseAwsLink(BaseOperatorLink):
    """Base Helper class for constructing AWS Console Link"""

    name: ClassVar[str]
    key: ClassVar[str]
    format_str: ClassVar[str]

    @staticmethod
    def get_aws_domain(aws_partition) -> str | None:
        if aws_partition == "aws":
            return "aws.amazon.com"
        elif aws_partition == "aws-cn":
            return "amazonaws.cn"
        elif aws_partition == "aws-us-gov":
            return "amazonaws-us-gov.com"

        return None

    def format_link(self, **kwargs) -> str:
        """
        Format AWS Service Link

        Some AWS Service Link should require additional escaping
        in this case this method should be overridden.
        """
        try:
            return self.format_str.format(**kwargs)
        except KeyError:
            return ""

    def get_link(
        self,
        operator,
        dttm: datetime | None = None,
        ti_key: TaskInstanceKey | None = None,
    ) -> str:
        """
        Link to Amazon Web Services Console.

        :param operator: airflow operator
        :param ti_key: TaskInstance ID to return link for
        :param dttm: execution date. Uses for compatibility with Airflow 2.2
        :return: link to external system
        """
        if ti_key is not None:
            conf = XCom.get_value(key=self.key, ti_key=ti_key)
        elif not dttm:
            conf = {}
        else:
            conf = XCom.get_one(
                key=self.key,
                dag_id=operator.dag.dag_id,
                task_id=operator.task_id,
                execution_date=dttm,
            )

        return self.format_link(**conf) if conf else ""

    @classmethod
    def persist(
        cls, context: Context, operator: BaseOperator, region_name: str, aws_partition: str, **kwargs
    ) -> None:
        """Store link information into XCom"""
        if not operator.do_xcom_push:
            return

        operator.xcom_push(
            context,
            key=cls.key,
            value={
                "region_name": region_name,
                "aws_domain": cls.get_aws_domain(aws_partition),
                **kwargs,
            },
        )
