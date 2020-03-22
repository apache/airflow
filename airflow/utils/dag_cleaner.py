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

from operator import or_

from pendulum import pendulum

from airflow import AirflowException
from airflow.models import TaskInstance, clear_task_instances
from airflow.models.dagbag import DagBag
from airflow.utils.helpers import ask_yesno
from airflow.utils.session import provide_session
from airflow.utils.state import State

# pylint: disable=too-many-locals,too-many-nested-blocks,too-many-branches


@provide_session
def clear(  # pylint: disable=too-many-arguments
    dag,
    start_date=None, end_date=None,
    only_failed=False,
    only_running=False,
    confirm_prompt=False,
    include_subdags=True,
    include_parentdag=True,
    reset_dag_runs=True,
    dry_run=False,
    session=None,
    get_tis=False,
    recursion_depth=0,
    max_recursion_depth=None,
    dag_bag=None,
):
    """
    Clears a set of task instances associated with the dag for
    a specified date range.

    :param start_date: The minimum execution_date to clear
    :type start_date: datetime.datetime or None
    :param end_date: The maximum exeuction_date to clear
    :type end_date: datetime.datetime or None
    :param only_failed: Only clear failed tasks
    :type only_failed: bool
    :param only_running: Only clear running tasks.
    :type only_running: bool
    :param confirm_prompt: Ask for confirmation
    :type confirm_prompt: bool
    :param include_subdags: Clear tasks in subdags and clear external tasks
        indicated by ExternalTaskMarker
    :type include_subdags: bool
    :param include_parentdag: Clear tasks in the parent dag of the subdag.
    :type include_parentdag: bool
    :param reset_dag_runs: Set state of dag to RUNNING
    :type reset_dag_runs: bool
    :param dry_run: Find the tasks to clear but don't clear them.
    :type dry_run: bool
    :param session: The sqlalchemy session to use
    :type session: sqlalchemy.orm.session.Session
    :param get_tis: Return the sqlachemy query for finding the TaskInstance without clearing the tasks
    :type get_tis: bool
    :param recursion_depth: The recursion depth of nested calls to DAG.clear().
    :type recursion_depth: int
    :param max_recursion_depth: The maximum recusion depth allowed. This is determined by the
        first encountered ExternalTaskMarker. Default is None indicating no ExternalTaskMarker
        has been encountered.
    :type max_recursion_depth: int
    :param dag_bag: The DagBag used to find the dags
    :type dag_bag: airflow.models.dagbag.DagBag
    """
    TI = TaskInstance
    tis = session.query(TI)
    if include_subdags:
        # Crafting the right filter for dag_id and task_ids combo
        conditions = []
        for current_dag in dag.subdags + [dag]:
            conditions.append(
                TI.dag_id.like(current_dag.dag_id) &
                TI.task_id.in_(current_dag.task_ids)
            )
        tis = tis.filter(or_(*conditions))
    else:
        tis = session.query(TI).filter(TI.dag_id == dag.dag_id)
        tis = tis.filter(TI.task_id.in_(dag.task_ids))

    if include_parentdag and dag.is_subdag:

        p_dag = dag.parent_dag.sub_dag(
            task_regex=r"^{}$".format(dag.dag_id.split('.')[1]),
            include_upstream=False,
            include_downstream=True)

        tis = tis.union(p_dag.clear(
            start_date=start_date, end_date=end_date,
            only_failed=only_failed,
            only_running=only_running,
            confirm_prompt=confirm_prompt,
            include_subdags=include_subdags,
            include_parentdag=False,
            reset_dag_runs=reset_dag_runs,
            get_tis=True,
            session=session,
            recursion_depth=recursion_depth,
            max_recursion_depth=max_recursion_depth,
            dag_bag=dag_bag
        ))

    if start_date:
        tis = tis.filter(TI.execution_date >= start_date)
    if end_date:
        tis = tis.filter(TI.execution_date <= end_date)
    if only_failed:
        tis = tis.filter(or_(
            TI.state == State.FAILED,
            TI.state == State.UPSTREAM_FAILED))
    if only_running:
        tis = tis.filter(TI.state == State.RUNNING)

    if include_subdags:
        from airflow.sensors.external_task_sensor import ExternalTaskMarker

        # Recursively find external tasks indicated by ExternalTaskMarker
        instances = tis.all()
        for ti in instances:
            if ti.operator == ExternalTaskMarker.__name__:
                ti.task = dag.get_task(ti.task_id)

                if recursion_depth == 0:
                    # Maximum recursion depth allowed is the recursion_depth of the first
                    # ExternalTaskMarker in the tasks to be cleared.
                    max_recursion_depth = ti.task.recursion_depth

                if recursion_depth + 1 > max_recursion_depth:
                    # Prevent cycles or accidents.
                    raise AirflowException("Maximum recursion depth {} reached for {} {}. "
                                           "Attempted to clear too many tasks "
                                           "or there may be a cyclic dependency."
                                           .format(max_recursion_depth,
                                                   ExternalTaskMarker.__name__, ti.task_id))
                ti.render_templates()
                external_tis = session.query(TI).filter(TI.dag_id == ti.task.external_dag_id,
                                                        TI.task_id == ti.task.external_task_id,
                                                        TI.execution_date ==
                                                        pendulum.parse(ti.task.execution_date))

                for tii in external_tis:
                    if not dag_bag:
                        dag_bag = DagBag()
                    external_dag = dag_bag.get_dag(tii.dag_id)
                    if not external_dag:
                        raise AirflowException("Could not find dag {}".format(tii.dag_id))
                    downstream = external_dag.sub_dag(
                        task_regex=r"^{}$".format(tii.task_id),
                        include_upstream=False,
                        include_downstream=True
                    )
                    tis = tis.union(downstream.clear(start_date=tii.execution_date,
                                                     end_date=tii.execution_date,
                                                     only_failed=only_failed,
                                                     only_running=only_running,
                                                     confirm_prompt=confirm_prompt,
                                                     include_subdags=include_subdags,
                                                     include_parentdag=False,
                                                     reset_dag_runs=reset_dag_runs,
                                                     get_tis=True,
                                                     session=session,
                                                     recursion_depth=recursion_depth + 1,
                                                     max_recursion_depth=max_recursion_depth,
                                                     dag_bag=dag_bag))

    if get_tis:
        return tis

    tis = tis.all()

    if dry_run:
        session.expunge_all()
        return tis

    # Do not use count() here, it's actually much slower than just retrieving all the rows when
    # tis has multiple UNION statements.
    count = len(tis)
    do_it = True
    if count == 0:
        return 0
    if confirm_prompt:
        ti_list = "\n".join([str(t) for t in tis])
        question = (
            "You are about to delete these {count} tasks:\n"
            "{ti_list}\n\n"
            "Are you sure? (yes/no): ").format(count=count, ti_list=ti_list)
        do_it = ask_yesno(question)

    if do_it:
        clear_task_instances(tis,
                             session,
                             dag=dag,
                             )
        if reset_dag_runs:
            dag.set_dag_runs_state(session=session,
                                   start_date=start_date,
                                   end_date=end_date,
                                   )
    else:
        count = 0
        print("Bail. Nothing was cleared.")

    session.commit()
    return count


def clear_dags(
    dags,
    start_date=None,
    end_date=None,
    only_failed=False,
    only_running=False,
    confirm_prompt=False,
    include_subdags=True,
    include_parentdag=False,
    reset_dag_runs=True,
    dry_run=False,
):
    """
    Clears a set of task instances associated with the list of dag for
    a specified date range.

    For more information look; `clear_dag`
    """
    all_tis = []
    for dag in dags:
        tis = dag.clear(
            start_date=start_date,
            end_date=end_date,
            only_failed=only_failed,
            only_running=only_running,
            confirm_prompt=False,
            include_subdags=include_subdags,
            include_parentdag=include_parentdag,
            reset_dag_runs=reset_dag_runs,
            dry_run=True)
        all_tis.extend(tis)

    if dry_run:
        return all_tis

    count = len(all_tis)
    do_it = True
    if count == 0:
        print("Nothing to clear.")
        return 0
    if confirm_prompt:
        ti_list = "\n".join([str(t) for t in all_tis])
        question = (
            "You are about to delete these {} tasks:\n"
            "{}\n\n"
            "Are you sure? (yes/no): ").format(count, ti_list)
        do_it = ask_yesno(question)

    if do_it:
        for dag in dags:
            dag.clear(start_date=start_date,
                      end_date=end_date,
                      only_failed=only_failed,
                      only_running=only_running,
                      confirm_prompt=False,
                      include_subdags=include_subdags,
                      reset_dag_runs=reset_dag_runs,
                      dry_run=False,
                      )
    else:
        count = 0
        print("Bail. Nothing was cleared.")
    return count
