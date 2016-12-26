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
import logging

import airflow.api

from airflow.api.common.experimental import trigger_dag as trigger
from airflow.exceptions import AirflowException
from airflow.www.app import csrf

from flask import (
    g, Markup, Blueprint, redirect, jsonify, abort, request, current_app, send_file
)

requires_authentication = airflow.api.api_auth.requires_authentication

api_experimental = Blueprint('api_experimental', __name__)

@csrf.exempt
@api_experimental.route('/dags/<string:dag_id>/dag_runs', methods=['POST'])
@requires_authentication
def trigger_dag(dag_id):
    """
    Trigger a new dag run for a Dag
    """
    data = request.get_json(force=True)

    run_id = None
    if 'run_id' in data:
        run_id = data['run_id']

    conf = None
    if 'conf' in data:
        conf = data['conf']

    try:
        dr = trigger.trigger_dag(dag_id, run_id, conf)
    except AirflowException as err:
        logging.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = 404
        return response

    if getattr(g, 'user', None):
        logging.info("User {} created {}".format(g.user, dr))

    response = jsonify(message="Created {}".format(dr))
    return response


@api_experimental.route('/test', methods=['GET'])
@requires_authentication
def test():
    return jsonify(status='OK')


@api_experimental.route('/dags/<string:dag_id>/tasks/<string:task_id>', methods=['GET'])
@requires_authentication
def task_info(dag_id, task_id):
    """Returns a JSON with a task's public instance variables. """
    from airflow.www.views import dagbag

    if dag_id not in dagbag.dags:
        response = jsonify(error='Dag {} not found'.format(dag_id))
        response.status_code = 404
        return response

    dag = dagbag.dags[dag_id]
    if not dag.has_task(task_id):
        response = (jsonify(error='Task {} not found in dag {}'
                    .format(task_id, dag_id)))
        response.status_code = 404
        return response

    task = dag.get_task(task_id)
    fields = {k: str(v) for k, v in vars(task).items() if not k.startswith('_')}
    return jsonify(fields)
