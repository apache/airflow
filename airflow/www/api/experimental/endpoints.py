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
#

from airflow.www.views import dagbag

from flask import Blueprint, jsonify

api_experimental = Blueprint('api_experimental', __name__)


@api_experimental.route('/dags/<string:dag_id>/tasks/<string:task_id>', methods=['GET'])
def task_info(dag_id, task_id):
    """Returns a JSON with a task's public instance variables. """
    if dag_id not in dagbag.dags:
        response = jsonify({'error': 'Dag {} not found'.format(dag_id)})
        response.status_code = 404
        return response

    dag = dagbag.dags[dag_id]
    if not dag.has_task(task_id):
        response = (jsonify({'error': 'Task {} not found in dag {}'
                    .format(task_id, dag_id)}))
        response.status_code = 404
        return response

    task = dag.get_task(task_id)
    fields = {k: str(v) for k, v in vars(task).items() if not k.startswith('_')}
    return jsonify(fields)
