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

import os
from http import HTTPStatus
import requests
import airflow.api
from airflow.api.common.experimental.mark_tasks import set_dag_run_final_state
from airflow.api.common.experimental import pool as pool_api
from airflow.api.common.experimental import trigger_dag as trigger
from airflow.api.common.experimental.get_dag_runs import get_dag_runs
from airflow.api.common.experimental.get_task import get_task
from airflow.api.common.experimental.get_task_instance import get_task_instance
from airflow.api.common.experimental.get_code import get_code
from airflow.api.common.experimental.get_dag_run_state import get_dag_run_state
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.strings import to_boolean
from airflow.utils import timezone
from airflow.www_rbac.app import csrf
from airflow import models
from airflow.utils.db import create_session
from .utils import get_cas_base_url
from flask import g, Blueprint, jsonify, request, url_for

_log = LoggingMixin().log

requires_authentication = airflow.api.API_AUTH.api_auth.requires_authentication

api_experimental = Blueprint('api_experimental', __name__)


@csrf.exempt
@api_experimental.route('/dags/<string:dag_id>/dag_runs', methods=['POST'])
@requires_authentication
def trigger_dag(dag_id):
    """
    Trigger a new dag run for a Dag with an execution date of now unless
    specified in the data.
    """
    data = request.get_json(force=True)

    run_id = None
    if 'run_id' in data:
        run_id = data['run_id']

    conf = None
    if 'conf' in data:
        conf = data['conf']

    execution_date = None
    if 'execution_date' in data and data['execution_date'] is not None:
        execution_date = data['execution_date']

        # Convert string datetime into actual datetime
        try:
            execution_date = timezone.parse(execution_date)
        except ValueError:
            error_message = (
                'Given execution date, {}, could not be identified '
                'as a date. Example date format: 2015-11-16T14:34:15+00:00'
                    .format(execution_date))
            _log.info(error_message)
            response = jsonify({'error': error_message})
            response.status_code = 400

            return response

    replace_microseconds = (execution_date is None)
    if 'replace_microseconds' in data:
        replace_microseconds = to_boolean(data['replace_microseconds'])

    try:
        dr = trigger.trigger_dag(
            dag_id,
            run_id,
            conf,
            execution_date,
            replace_microseconds)
    except AirflowException as err:
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response

    if getattr(g, 'user', None):
        _log.info("User {} created {}".format(g.user, dr))

    response = jsonify(
        message="Created {}".format(dr),
        execution_date=dr.execution_date.isoformat())
    return response


@api_experimental.route('/dags/<string:dag_id>/dag_runs', methods=['GET'])
@requires_authentication
def dag_runs(dag_id):
    """
    Returns a list of Dag Runs for a specific DAG ID.
    :query param state: a query string parameter '?state=queued|running|success...'
    :param dag_id: String identifier of a DAG
    :return: List of DAG runs of a DAG with requested state,
    or all runs if the state is not specified
    """
    try:
        state = request.args.get('state')
        dagruns = get_dag_runs(dag_id, state)
    except AirflowException as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = 400
        return response

    return jsonify(dagruns)


@api_experimental.route('/test', methods=['GET'])
@requires_authentication
def test():
    return jsonify(status='OK')


@api_experimental.route('/dags/<string:dag_id>/code', methods=['GET'])
@requires_authentication
def get_dag_code(dag_id):
    """Return python code of a given dag_id."""
    try:
        return get_code(dag_id)
    except AirflowException as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response


@api_experimental.route(
    '/dags/<string:dag_id>/tasks/<string:task_id>',
    methods=['GET'])
@requires_authentication
def task_info(dag_id, task_id):
    """Returns a JSON with a task's public instance variables. """
    try:
        info = get_task(dag_id, task_id)
    except AirflowException as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response

    # JSONify and return.
    fields = {k: str(v)
              for k, v in vars(info).items()
              if not k.startswith('_')}
    return jsonify(fields)


def docasInvaild(pkg_name):
    cas_base_url = get_cas_base_url()
    connection_model = models.connection.Connection
    url = "{}/cas/invalid-curve".format(
        cas_base_url.get_uri() if isinstance(cas_base_url, connection_model) else cas_base_url)
    data = {'pkg_name': pkg_name}
    try:
        resp = requests.post(url=url, data=data)
        if resp.status_code != HTTPStatus.OK:
            raise Exception(resp.content)
    except Exception as e:
        raise AirflowException(str(e))


@api_experimental.route(
    '/dags/<string:dag_id>/tasks/<string:task_id>/<string:execution_date>/confirm',
    methods=['POST'])
@requires_authentication
def double_confirm_task(dag_id, task_id, execution_date):
    try:
        execution_date = timezone.parse(execution_date)
    except ValueError:
        error_message = (
            'Given execution date, {}, could not be identified '
            'as a date. Example date format: 2015-11-16T14:34:15+00:00'
                .format(execution_date))
        _log.info(error_message)
        response = jsonify({'error': error_message})
        response.status_code = 400

        return response
    try:
        params = request.get_json(force=True)  # success failed
        final_state = params.get('final_state', None)
        task = get_task_instance(dag_id, task_id, execution_date)
        if not task.result:
            raise AirflowException(u"分析结果还没有生成，请等待分析结果生成后再进行二次确认")
        if not final_state or final_state not in ['OK', 'NOK']:
            raise AirflowException("二次确认参数未定义或数值不正确!")
        if task.result != final_state:
            # 分析结果与二次确认结果不同
            docasInvaild(task.pkg_name)

    except AirflowException as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response

    set_dag_run_final_state(
        task_id=task_id,
        dag_id=dag_id,
        execution_date=execution_date,
        final_state=final_state)
    return jsonify({'response': 'ok'})


# ToDo: Shouldn't this be a PUT method?
@api_experimental.route(
    '/dags/<string:dag_id>/paused/<string:paused>',
    methods=['GET'])
@requires_authentication
def dag_paused(dag_id, paused):
    """(Un)pauses a dag"""

    DagModel = models.DagModel
    with create_session() as session:
        orm_dag = (
            session.query(DagModel)
                .filter(DagModel.dag_id == dag_id).first()
        )
        if paused == 'true':
            orm_dag.is_paused = True
        else:
            orm_dag.is_paused = False
        session.merge(orm_dag)
        session.commit()

    return jsonify({'response': 'ok'})


@api_experimental.route(
    '/dags/<string:dag_id>/dag_runs/<string:execution_date>/tasks/<string:task_id>',
    methods=['GET'])
@requires_authentication
def task_instance_info(dag_id, execution_date, task_id):
    """
    Returns a JSON with a task instance's public instance variables.
    The format for the exec_date is expected to be
    "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15". This will
    of course need to have been encoded for URL in the request.
    """

    # Convert string datetime into actual datetime
    try:
        execution_date = timezone.parse(execution_date)
    except ValueError:
        error_message = (
            'Given execution date, {}, could not be identified '
            'as a date. Example date format: 2015-11-16T14:34:15+00:00'
                .format(execution_date))
        _log.info(error_message)
        response = jsonify({'error': error_message})
        response.status_code = 400

        return response

    try:
        info = get_task_instance(dag_id, task_id, execution_date)
    except AirflowException as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response

    # JSONify and return.
    fields = {k: str(v)
              for k, v in vars(info).items()
              if not k.startswith('_')}
    return jsonify(fields)


@api_experimental.route(
    '/dags/<string:dag_id>/dag_runs/<string:execution_date>',
    methods=['GET'])
@requires_authentication
def dag_run_status(dag_id, execution_date):
    """
    Returns a JSON with a dag_run's public instance variables.
    The format for the exec_date is expected to be
    "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15". This will
    of course need to have been encoded for URL in the request.
    """

    # Convert string datetime into actual datetime
    try:
        execution_date = timezone.parse(execution_date)
    except ValueError:
        error_message = (
            'Given execution date, {}, could not be identified '
            'as a date. Example date format: 2015-11-16T14:34:15+00:00'.format(
                execution_date))
        _log.info(error_message)
        response = jsonify({'error': error_message})
        response.status_code = 400

        return response

    try:
        info = get_dag_run_state(dag_id, execution_date)
    except AirflowException as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response

    return jsonify(info)


@api_experimental.route('/latest_runs', methods=['GET'])
@requires_authentication
def latest_dag_runs():
    """Returns the latest DagRun for each DAG formatted for the UI. """
    from airflow.models import DagRun
    dagruns = DagRun.get_latest_runs()
    payload = []
    for dagrun in dagruns:
        if dagrun.execution_date:
            payload.append({
                'dag_id': dagrun.dag_id,
                'execution_date': dagrun.execution_date.isoformat(),
                'start_date': ((dagrun.start_date or '') and
                               dagrun.start_date.isoformat()),
                'dag_run_url': url_for('Airflow.graph', dag_id=dagrun.dag_id,
                                       execution_date=dagrun.execution_date)
            })
    # old flask versions dont support jsonifying arrays
    return jsonify(items=payload)


@api_experimental.route('/pools/<string:name>', methods=['GET'])
@requires_authentication
def get_pool(name):
    """Get pool by a given name."""
    try:
        pool = pool_api.get_pool(name=name)
    except AirflowException as err:
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response
    else:
        return jsonify(pool.to_json())


@api_experimental.route('/pools', methods=['GET'])
@requires_authentication
def get_pools():
    """Get all pools."""
    try:
        pools = pool_api.get_pools()
    except AirflowException as err:
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response
    else:
        return jsonify([p.to_json() for p in pools])


@csrf.exempt
@api_experimental.route('/pools', methods=['POST'])
@requires_authentication
def create_pool():
    """Create a pool."""
    params = request.get_json(force=True)
    try:
        pool = pool_api.create_pool(**params)
    except AirflowException as err:
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response
    else:
        return jsonify(pool.to_json())


@csrf.exempt
@api_experimental.route('/pools/<string:name>', methods=['DELETE'])
@requires_authentication
def delete_pool(name):
    """Delete pool."""
    try:
        pool = pool_api.delete_pool(name=name)
    except AirflowException as err:
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response
    else:
        return jsonify(pool.to_json())
