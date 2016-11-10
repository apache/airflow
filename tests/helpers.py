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

from airflow.models import DagBag


def mock_dag_bag(*args):
    """Given instances of the ``DAG`` class, returns a ``DagBag`` instance
    (for use with ``mock.patch``) that contains the DAGs."""
    for dag in args:
        if not hasattr(dag, 'is_subdag'):
            dag.is_subdag = False  # Normally set in ``DagBag.bag_dag``.

    dag_bag = DagBag()

    for dag in args:
        dag_bag.bag_dag(dag)

    return dag_bag
