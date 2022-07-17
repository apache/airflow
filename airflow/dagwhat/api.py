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

from airflow import DAG
from airflow.dagwhat.base import *


__all__ = [
    'assert_that',
    'given',
    'task',
    'tasks',
    'any_task',
    'succeeds',
    'fails',
    'runs',
    'does_not_run',
    'may_run',
]


def assert_that(test_case: TaskTestCheckBuilder):
    run_check(test_case.build())


def given(dag: DAG) -> 'DagTest':
    return DagTest(dag)

##############################################################################
#### TASK SELECTORS ########
##############################################################################
def task(id):
    return TaskGroupSelector(ids=[id], group_is=TaskGroupSelector.ALL)


def tasks(*ids):
    return TaskGroupSelector(ids=ids, group_is=TaskGroupSelector.ANY)


def any_task(with_id=None, with_operator=None):
    return TaskGroupSelector(ids=[with_id], operators=[with_operator], group_is=TaskGroupSelector.ANY)


def all_tasks(with_id=None, with_operator=None):
    return TaskGroupSelector(ids=[with_id], operators=[with_operator], group_is=TaskGroupSelector.ALL)

##############################################################################
#### END TASK SELECTORS ########
##############################################################################

##############################################################################
#### OUTCOME CHECKERS ########
##############################################################################

def succeeds() -> TaskOutcome:
    return TaskOutcome(TaskOutcome.SUCCESS)

def fails() -> TaskOutcome:
    return TaskOutcome(TaskOutcome.FAILURE)

def runs() -> TaskOutcome:
    return TaskOutcome(TaskOutcome.ANY)

# TODO(pabloem): Open question: TaskOutcome is used to generate test conditions.
#   It is ALSO used to generate CHECKS.

def does_not_run() -> TaskOutcome:
    return TaskOutcome(TaskOutcome.NOT_RUN)

def may_run() -> TaskOutcome:
    return TaskOutcome(TaskOutcome.MAY_RUN)

##############################################################################
#### END OUTCOME CHECKERS ########
##############################################################################
