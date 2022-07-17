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
import typing

from airflow.models import DAG
from airflow.models import Operator


TaskIdType = str


def run_check(check: 'FinalTaskTestCheck'):
    dag = check.dag
    condition_tester = check.task_test_condition
    validations_to_apply = check.validation_chain

    # TODO(pabloem): Support multiple test conditions.
    #  The code below assumes only single test conditions.
    task_selector, assumed_outcome = condition_tester.condition_chain[0]

    for matching_taskgroup in task_selector.generate_task_groups(dag):
        dag_with_assumption = apply_assumption(matching_taskgroup, assumed_outcome, dag)


def apply_assumption(matching_taskgroup: typing.List[typing.Tuple[TaskIdType, Operator]],
                     assumed_outcome: 'TaskOutcome', dag: DAG):
    for task_id, op in matching_taskgroup:
        # TODO(pabloem): Verify that it is POSSIBLE to apply the current
        #  assumption given the current DAG state.
        pass

class TaskOutcome:
    SUCCESS = object()
    FAILURE = object()
    ANY = object()  # TODO(pabloem): Document - this represents any outcome: Failure or success.
    NOT_RUN = object() # TODO(pabloem): Document - this represents the task never running in a DAG run.
    MAY_RUN = object() # TODO(pabloem): Document - this represents the opposite of NOT RUN
    # TODO(pabloem): Support tasks failing and being retried, etc.

    def __init__(self, outcome):
        self.outcome = outcome

class TaskGroupSelector:
    ANY = object()
    ALL = object()
    def __init__(self,
                 ids=None,
                 operators: typing.Set[typing.Type[Operator]] = None,
                 group_is: typing.Union['TaskGroupSelector.ALL',
                                        'TaskGroupSelector.ANY'] = None):
        self.ids = ids
        self.operators = operators
        self.group_is = group_is

    def generate_task_groups(self, dag: DAG) -> typing.Iterable[typing.List[typing.Tuple[TaskIdType, Operator]]]:
        def id_matches(real_id, matching_id):
            # TODO(pabloem): support wildcard matching
            return real_id == matching_id

        matching_tasks = []
        for task_id in dag.task_dict:
            operator = dag.task_dict[task_id]
            if any(isinstance(operator, allowed_operator) for allowed_operator in self.operators):
                matching_tasks.append((task_id, dag.task_dict[task_id]))
                continue
            if any(id_matches(task_id, matching_id) for matching_id in self.ids):
                matching_tasks.append((task_id, dag.task_dict[task_id]))
                continue

        # Verify that for exact ID matching, we have one task per id
        if self.ids and len(self.ids) != len(matching_tasks):
            found_ids = {id for id, _ in matching_tasks}
            raise ValueError(
                'Unable to match all expected IDs to tasks in the DAG. '
                'Unmatched IDs: %r' % set(self.ids).difference(found_ids))

        if self.group_is == TaskGroupSelector.ANY:
            return [[id_op] for id_op in matching_tasks]
        else:
            assert self.group_is == TaskGroupSelector.ALL
            return [matching_tasks]


class TaskTestConditionGenerator:
    """TODO(pabloem): Must document.

    When defining a DagTest, we establish invariants for tasks.

    A `TaskTestConditionGenerator` receives a specfication that allows it to
    generate test cases (or test situations).
    """

    def __init__(self, task_selector):
        self.task_selector = task_selector


class FinalTaskTestCheck:
    def __init__(self, dag: DAG, task_test_condition: 'TaskTestBuilder', validation_chain):
        self.dag = dag
        self.task_test_condition = task_test_condition
        self.validation_chain = validation_chain


class TaskTestCheckBuilder:
    """TODO(pabloem): Must document"""

    def __init__(self, task_test_condition: 'TaskTestBuilder'):
        self.task_test_condition = task_test_condition
        self.validation_chain = []
        self.checked = False

    def _add_check(self, task_or_dag_selector, task_or_dag_outcome) -> 'TaskTestCheckBuilder':
        self.validation_chain.append((task_or_dag_selector, task_or_dag_outcome))
        return self

    def __del__(self):
        if not self.checked:
            raise AssertionError(
                "A dagwhat test has been defined, but was not tested.\n\t"
                "Please wrap your test with assert_that to make sure checks "
                "will run.")

    def _mark_checked(self):
        self.checked = True
        self.task_test_condition._mark_checked()

    def build(self):
        self._mark_checked()
        return FinalTaskTestCheck(self.task_test_condition.dag_test.dag, self.task_test_condition, self.validation_chain)

class TaskTestBuilder:
    """TODO(pabloem): Must document"""

    def __init__(self, dag_test: 'DagTest', task_selector: TaskGroupSelector, outcome: TaskOutcome):  # TODO(pabloem): How do we make an ENUM? lol
        self.dag_test = dag_test
        self.condition_chain = [(task_selector, outcome)]

        # Dagwhat supports multi-and conditions or multi-or conditions.
        # Because mixed AND / OR evaluations are not associative, supporting
        # a mix of these conditions would create ambiguity in the API.
        # TODO(pabloem): Add support to multi-OR conditions, not just multi-AND
        #  conditions. Idea: We can take advantage of a typesystem by defining
        #  'AdjunctiveTaskTestBuilder' and 'DisjunctiveTaskTestBuilder' as
        #  subclasses of 'TaskTestBuilder'. This would 'force' users to use
        #  appropriate methods from the typesystem and checked statically
        #  instead of at runtime (though runtime check should follow the
        #  static check nearly immediately).
        self.condition_chain_method = 'AND'
        self.checked = False

    def _mark_checked(self):
        self.checked = True
        self.dag_test._mark_checked()

    def __del__(self):
        if not self.checked:
            raise AssertionError(
                "A dagwhat test has been defined, but was not tested.\n\t"
                "Please wrap your test with assert_that to make sure checks "
                "will run.")

    def and_(self, task_selector, outcome) -> 'TaskTestBuilder':
        # self.condition_chain.append((task_selector, outcome))
        # return self
        raise NotImplementedError(
            'Additive test conditions are not yet supported.')

    def then(self, task_or_dag_selector, task_or_dag_outcome) -> TaskTestCheckBuilder:
        check_builder = TaskTestCheckBuilder(self)
        check_builder._add_check(task_or_dag_selector, task_or_dag_outcome)
        return check_builder


class DagTest:
    """TODO(pabloem): Must document"""

    def __init__(self, dag: DAG):
        self.dag = dag
        self.checked = False

    def _mark_checked(self):
        self.checked = True

    def __del__(self):
        if not self.checked:
            raise AssertionError(
                "A dagwhat test has been defined, but was not tested.\n\t"
                "Please wrap your test with assert_that to make sure checks "
                "will run.")

    def when(self, task_selector, outcome) -> TaskTestBuilder:
        return TaskTestBuilder(task_selector, outcome)
