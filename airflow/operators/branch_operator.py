# -*- coding: utf-8 -*-
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

from airflow.models import BaseOperator, SkipMixin

from typing import Union, Iterable, Dict, Set


class BranchOperator(BaseOperator, SkipMixin):
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

    def choose_branch(self, context: Dict) -> Union[str, Iterable[str]]:
        """
        Subclasses should implement this, running whatever logic is
        necessary to choose a branch and returning a task_id or list of
        task_ids.

        :param context: Context dictionary as passed to execute()
        :type context: dict
        """
        raise NotImplementedError

    def execute(self, context: Dict):
        branch_task_ids = self.choose_branch(context)
        self.log.info("Following branch %s", branch_task_ids)

        if isinstance(branch_task_ids, str):
            branch_task_ids = [branch_task_ids]

        downstream_tasks = context['task'].downstream_list

        if downstream_tasks:
            # Also check downstream tasks of the branch task. In case the task to skip
            # is also a downstream task of the branch task, we exclude it from skipping.
            branch_downstream_task_ids = set()  # type: Set[str]
            for b in branch_task_ids:
                branch_downstream_task_ids.update(context["dag"].
                                                  get_task(b).
                                                  get_flat_relative_ids(upstream=False))

            skip_tasks = [t for t in downstream_tasks
                          if t.task_id not in branch_task_ids and
                          t.task_id not in branch_downstream_task_ids]

            self.log.info("Skipping tasks %s", [t.task_id for t in skip_tasks])
            self.skip(context['dag_run'], context['ti'].execution_date, skip_tasks)
