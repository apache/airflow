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
import warnings
from typing import NamedTuple
from unittest import mock

import attr

from airflow.models import TaskInstance
from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.utils.decorators import apply_defaults


# Namedtuple for testing purposes
class MockNamedTuple(NamedTuple):
    var1: str
    var2: str


class MockOperator(BaseOperator):
    """Operator for testing purposes."""

    template_fields = ("arg1", "arg2")

    @apply_defaults
    def __init__(self, arg1: str = "", arg2: str = "", **kwargs):
        super().__init__(**kwargs)
        self.arg1 = arg1
        self.arg2 = arg2

    def execute(self, context):
        pass


class AirflowLink(BaseOperatorLink):
    """
    Operator Link for Apache Airflow Website
    """
    name = 'airflow'

    def get_link(self, operator, dttm):
        return 'should_be_overridden'


class Dummy2TestOperator(BaseOperator):
    """
    Example of an Operator that has an extra operator link
    and will be overridden by the one defined in tests/plugins/test_plugin.py
    """
    operator_extra_links = (
        AirflowLink(),
    )


class Dummy3TestOperator(BaseOperator):
    """
    Example of an operator that has no extra Operator link.
    An operator link would be added to this operator via Airflow plugin
    """
    operator_extra_links = ()


@attr.s(auto_attribs=True)
class CustomBaseIndexOpLink(BaseOperatorLink):
    index: int = attr.ib()

    @property
    def name(self) -> str:
        return 'BigQuery Console #{index}'.format(index=self.index + 1)

    def get_link(self, operator, dttm):
        ti = TaskInstance(task=operator, execution_date=dttm)
        search_queries = ti.xcom_pull(task_ids=operator.task_id, key='search_query')
        if not search_queries:
            return None
        if len(search_queries) < self.index:
            return None
        search_query = search_queries[self.index]
        return f'https://console.cloud.google.com/bigquery?j={search_query}'


class CustomOpLink(BaseOperatorLink):
    name = 'Google Custom'

    def get_link(self, operator, dttm):
        ti = TaskInstance(task=operator, execution_date=dttm)
        search_query = ti.xcom_pull(task_ids=operator.task_id, key='search_query')
        return f'http://google.com/custom_base_link?search={search_query}'


class CustomOperator(BaseOperator):

    template_fields = ['bash_command']

    @property
    def operator_extra_links(self):
        """
        Return operator extra links
        """
        if isinstance(self.bash_command, str) or self.bash_command is None:
            return (
                CustomOpLink(),
            )
        return (
            CustomBaseIndexOpLink(i) for i, _ in enumerate(self.bash_command)
        )

    @apply_defaults
    def __init__(self, bash_command=None, **kwargs):
        super().__init__(**kwargs)
        self.bash_command = bash_command

    def execute(self, context):
        self.log.info("Hello World!")
        context['task_instance'].xcom_push(key='search_query', value="dummy_value")


class GoogleLink(BaseOperatorLink):
    """
    Operator Link for Apache Airflow Website for Google
    """
    name = 'google'
    operators = [Dummy3TestOperator, CustomOperator]

    def get_link(self, operator, dttm):
        return 'https://www.google.com'


class AirflowLink2(BaseOperatorLink):
    """
    Operator Link for Apache Airflow Website for 1.10.5
    """
    name = 'airflow'
    operators = [Dummy2TestOperator, Dummy3TestOperator]

    def get_link(self, operator, dttm):
        return 'https://airflow.apache.org/1.10.5/'


class GithubLink(BaseOperatorLink):
    """
    Operator Link for Apache Airflow GitHub
    """
    name = 'github'

    def get_link(self, operator, dttm):
        return 'https://github.com/apache/airflow'


class MockHiveOperator(HiveOperator):
    def __init__(self, *args, **kwargs):
        self.run = mock.MagicMock()
        super().__init__(*args, **kwargs)


class DeprecatedOperator(BaseOperator):
    @apply_defaults
    def __init__(self, **kwargs):
        warnings.warn("This operator is deprecated.", DeprecationWarning, stacklevel=4)
        super().__init__(**kwargs)

    def execute(self, context):
        pass
