# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, Pool
from airflow.utils.decorators import apply_defaults
from airflow.utils.db import provide_session
from airflow.executors import DEFAULT_EXECUTOR


class SubDagOperator(BaseOperator):

    template_fields = tuple()
    ui_color = '#555'
    ui_fgcolor = '#fff'

    @provide_session
    @apply_defaults
    def __init__(
            self,
            subdag,
            executor=DEFAULT_EXECUTOR,
            *args, **kwargs):
        """
        Yo dawg. This runs a sub dag. By convention, a sub dag's dag_id
        should be prefixed by its parent and a dot. As in `parent.child`.

        :param subdag: the DAG object to run as a subdag of the current DAG.
        :type subdag: airflow.DAG
        :param dag: the parent DAG
        :type subdag: airflow.DAG
        """
        if 'dag' not in kwargs:
            raise AirflowException("Please pass in the `dag` param")
        dag = kwargs['dag']
        session = kwargs.pop('session')
        super(SubDagOperator, self).__init__(*args, **kwargs)

        # validate subdag name
        if dag.dag_id + '.' + kwargs['task_id'] != subdag.dag_id:
            raise AirflowException(
                "The subdag's dag_id should have the form "
                "'{{parent_dag_id}}.{{this_task_id}}'. Expected "
                "'{d}.{t}'; received '{rcvd}'.".format(
                    d=dag.dag_id, t=kwargs['task_id'], rcvd=subdag.dag_id))

        # validate that subdag operator and subdag tasks don't have a
        # pool conflict
        if self.pool:
            pool = (
                session
                .query(Pool)
                .filter(Pool.slots == 1)
                .filter(Pool.pool == self.pool)
                .first()
            )
            conflicts = [t for t in subdag.tasks if t.pool == self.pool]
            if pool and any(t.pool == self.pool for t in subdag.tasks):
                raise AirflowException(
                    'SubDagOperator {sd} and subdag task{plural} {t} both use '
                    'pool {p}, but the pool only has 1 slot. The subdag tasks'
                    'will never run.'.format(
                        sd=self.task_id,
                        plural=len(conflicts) > 1,
                        t=', '.join(t.task_id for t in conflicts),
                        p=self.pool
                    )
                )

        self.subdag = subdag
        self.executor = executor

    def execute(self, context):
        ed = context['execution_date']
        self.subdag.run(
            start_date=ed, end_date=ed, donot_pickle=True,
            executor=self.executor)
