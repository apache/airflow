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

from airflow import AirflowException
from airflow.models import DagBag, DAG


def delete_dag(dag_id, dag_bag=None):
    """

    :param dag: DAG to be deleted
    :param session: orm session
    :return: Returns true if dagrun is scheduled and successfully deleted
             Returns false if dagrun does not exist
    """

    if dag_id is None:
        return False

    dag_bag = dag_bag or DagBag()

    if dag_id not in dag_bag.dags:
        print ("dag not found")
        raise AirflowException("Dag id {} not found".format(dag_id))

    dag = dag_bag.get_dag(dag_id)

    alldags = [dag]
    alldags.extend(dag.subdags)

    for dag in alldags:
        dag.delete()

    return True

def check_delete_dag(dag_id):
    return DAG.find_deleted_entities(dag_id)
