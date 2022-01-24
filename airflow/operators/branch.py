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
"""Branching operators"""

from typing import Iterable, Union

from airflow.models import BaseOperator
from airflow.models.skipmixin import SkipMixin
from airflow.utils.context import Context


class BaseBranchOperator(BaseOperator, SkipMixin):
    """
    This is a base class for creating operators with branching functionality,
    similarly to BranchPythonOperator.

    Users should subclass this operator and implement the function
    `choose_branch(self, context)`. This should run whatever business logic
    is needed to determine the branch, and return either the task_id for
    a single task (as a str) or a list of task_ids.

    The operator will continue with the returned task_id(s), and all other
    tasks directly downstream of this operator will be skipped.
    """

    def choose_branch(self, context: Context) -> Union[str, Iterable[str]]:
        """
        Subclasses should implement this, running whatever logic is
        necessary to choose a branch and returning a task_id or list of
        task_ids.

        :param context: Context dictionary as passed to execute()
        """
        raise NotImplementedError

    def execute(self, context: Context):
        branches_to_execute = self.choose_branch(context)
        self.skip_all_except(context['ti'], branches_to_execute)
        return branches_to_execute
