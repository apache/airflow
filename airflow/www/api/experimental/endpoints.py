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
import airflow.api
from airflow.api.common.experimental import delete_dag as delete
from airflow.api.common.experimental import pool as pool_api
from airflow.api.common.experimental import trigger_dag as trigger
from airflow.api.common.experimental.dag_runs import get_all_dag_runs, get_dag_runs
from airflow.api.common.experimental.task_instance import get_task_instance, get_all_task_instances
from airflow.api.common.experimental.dags import get_dags, get_dag
from airflow.api.common.experimental.get_task import get_task, get_task_as_dict
from airflow.api.common.experimental.get_tasks import get_tasks
from airflow.api.common.experimental.get_code import get_code
from airflow.api.common.experimental.get_task_logs import get_task_logs
from airflow.exceptions import AirflowException
from airflow.exceptions import DagNotFound
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils import timezone
from airflow.www.app import csrf
from airflow import models

from flask import g, Blueprint, jsonify, request, url_for

_log = LoggingMixin().log

requires_authentication = airflow.api.API_AUTH.api_auth.requires_authentication

api_experimental = Blueprint('api_experimental', __name__)


@api_experimental.route(
    '/dags/<string:dag_id>/dag_runs/<string:execution_date>/tasks/<string:task_id>/logs',
    methods=['GET'])
@requires_authentication
def logs(dag_id, execution_date, task_id):
    """
    Return logs for the specified task identified by dag_id, execution_date and task_id
    """

    try:
        execution_date = timezone.parse(execution_date)
    except ValueError:
        error_message = (
            'Given execution date, {}, could not be identified '
            'as a date. Example date format: 2015-11-16T14:34:15+00:00'
            .format(execution_date))
        response = jsonify({'error': error_message})
        response.status_code = 400
        return response

    try:
        log = get_task_logs(dag_id, task_id, execution_date)
    except AirflowException as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response
    except AttributeError as e:
        error_message = ["Unable to read logs.\n{}\n".format(str(e))]
        metadata = {}
        metadata['end_of_log'] = True
        return jsonify(message=error_message, error=True, metadata=metadata)

    return log


@api_experimental.route('/dag_runs', methods=['GET'])
@requires_authentication
def dag_runs_filter():
    """
    Return the list of all dag_runs

    :query param state: a query string parameter '?state=queued|running|success...'
    :query param state_ne: a query string parameter '?state_ne=queued|running|success...'
    :query param execution_date_before: a query string parameter to find all runs before provided date,
    should be in format "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15"
    :query param execution_date_after: a query string parameter to find all runs after provided date,
    should be in format "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15"
    :query param dag_id: String identifier of a DAG
    :return: List of DAG runs of a DAG with requested state,
    """
    state = request.args.get('state')
    state_ne = request.args.get('state_ne')
    execution_date_before = request.args.get('execution_date_before')
    execution_date_after = request.args.get('execution_date_after')
    dag_id = request.args.get('dag_id')

    dagruns = get_all_dag_runs(dag_id=dag_id, state=state, state_ne=state_ne,
                               execution_date_before=execution_date_before,
                               execution_date_after=execution_date_after)

    return jsonify(dagruns)


@api_experimental.route('/task_instances', methods=['GET'])
@requires_authentication
def task_instances_filter():
    """
    Return the list of all dag_runs
    :query param state: a query string parameter '?state=queued|running|success...'
    :query param state_ne: a query string parameter '?state_ne=queued|running|success...'
    :query param execution_date_before: a query string parameter to find all runs before provided date,
    should be in format "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15".'
    :query param execution_date_after: a query string parameter to find all runs after provided date,
    should be in format "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15".'
    :query param dag_id: String identifier of a DAG
    :query param task_id: String identifier of a task
    :return: List of task instances
    """
    state = request.args.get('state')
    state_ne = request.args.get('state_ne')
    execution_date_before = request.args.get('execution_date_before')
    execution_date_after = request.args.get('execution_date_after')
    dag_id = request.args.get('dag_id')
    task_id = request.args.get('task_id')

    task_instances = get_all_task_instances(dag_id=dag_id, state=state, state_ne=state_ne,
                                            execution_date_before=execution_date_before,
                                            execution_date_after=execution_date_after, task_id=task_id)

    return jsonify(task_instances)


@api_experimental.route('/dags', methods=['GET'])
@requires_authentication
def get_all_dags():
    """
    Returns a list of Dags
    :query param is_paused: a query string parameter '?is_paused=true|false'
    :return: List of all DAGs
    """
    is_paused = request.args.get('is_paused')
    if is_paused:
        is_paused = is_paused.lower()
        is_paused = 1 if is_paused == 'true' else 0

    is_subdag = request.args.get('is_subdag')
    if is_subdag:
        is_subdag = is_subdag.lower()
        is_subdag = 1 if is_subdag == 'true' else 0

    is_active = request.args.get('is_active')
    if is_active:
        is_active = is_active.lower()
        is_active = 1 if is_active == 'true' else 0

    scheduler_lock = request.args.get('scheduler_lock')
    if scheduler_lock:
        scheduler_lock = scheduler_lock.lower()
        scheduler_lock = 1 if scheduler_lock == 'true' else 0

    dag_list = get_dags(is_paused=is_paused, is_subdag=is_subdag, is_active=is_active,
                        scheduler_lock=scheduler_lock)

    return jsonify(dag_list)


@api_experimental.route('/dags/<string:dag_id>', methods=['GET'])
@requires_authentication
def get_dag_info(dag_id):
    """
    Returns information for a single dag
    """
    dag = None
    try:
        dag = get_dag(dag_id)
    except DagNotFound as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = 404
        return response
    return jsonify(dag)


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

    try:
        dr = trigger.trigger_dag(dag_id, run_id, conf, execution_date)
    except AirflowException as err:
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response

    if getattr(g, 'user', None):
        _log.info("User %s created %s", g.user, dr)

    response = jsonify(message="Created {}".format(dr), execution_date=dr.execution_date.isoformat())
    return response


@csrf.exempt
@api_experimental.route('/dags/<string:dag_id>', methods=['DELETE'])
@requires_authentication
def delete_dag(dag_id):
    """
    Delete all DB records related to the specified Dag.
    """
    try:
        count = delete.delete_dag(dag_id)
    except AirflowException as err:
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response
    return jsonify(message="Removed {} record(s)".format(count), count=count)


@api_experimental.route('/dags/<string:dag_id>/dag_runs', methods=['GET'])
@requires_authentication
def dag_runs(dag_id):
    """
    Returns a list of Dag Runs for a specific DAG ID.
    :query param state: a query string parameter '?state=queued|running|success...'
    :query param state_ne: a query string parameter '?state_ne=queued|running|success...'
    :query param execution_date_before: a query string parameter to find all runs before provided date,
    should be in format "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15"'
    :query param execution_date_after: a query string parameter to find all runs after provided date,
    should be in format "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15"'
    :param dag_id: String identifier of a DAG
    :return: List of DAG runs of a DAG with requested state,
    or all runs if the state is not specified
    """
    try:
        state = request.args.get('state')
        state_ne = request.args.get('state_ne')
        execution_date_before = request.args.get('execution_date_before')
        execution_date_after = request.args.get('execution_date_after')
        dagruns = get_dag_runs(dag_id, state=state, state_ne=state_ne,
                               execution_date_before=execution_date_before,
                               execution_date_after=execution_date_after)
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


@api_experimental.route('/dags/<string:dag_id>/tasks', methods=['GET'])
@requires_authentication
def tasks(dag_id):
    """Returns a JSON with all tasks associated with the dag_id. """

    task_list = list()
    try:
        task_ids = get_tasks(dag_id)
        for task_id in task_ids:
            task_list.append(get_task_as_dict(dag_id, task_id))

    except AirflowException as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response

    return jsonify(task_list)


@api_experimental.route('/dags/<string:dag_id>/tasks/<string:task_id>', methods=['GET'])
@requires_authentication
def task_info(dag_id, task_id):
    """Returns a JSON with a task's public instance variables. """
    try:
        info = get_task_as_dict(dag_id, task_id)
    except AirflowException as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response

    # JSONify and return.
    return jsonify(info)


# ToDo: Shouldn't this be a PUT method?
@api_experimental.route('/dags/<string:dag_id>/paused/<string:paused>', methods=['GET'])
@requires_authentication
def dag_paused(dag_id, paused):
    """(Un)pauses a dag"""

    is_paused = True if paused == 'true' else False

    models.DagModel.get_dagmodel(dag_id).set_is_paused(
        is_paused=is_paused,
    )

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
        task_instance = get_task_instance(dag_id, task_id, execution_date)
        task = get_task(dag_id, task_id)
    except AirflowException as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response

    # JSONify and return.
    fields = {k: str(v)
              for k, v in vars(task_instance).items()
              if not k.startswith('_')}
    fields.update({
        'try_number': task_instance._try_number,
        'upstream_task_ids': list(task._upstream_task_ids),
        'downstream_task_ids': list(task._downstream_task_ids)})
    return jsonify(fields)


@api_experimental.route(
    '/dags/<string:dag_id>/dag_runs/<string:execution_date>',
    methods=['GET'])
@requires_authentication
def dag_run(dag_id, execution_date):
    """
    Returns a JSON with a dag_run's public instance variables.
    The format for the exec_date is expected to be
    "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15". This will
    of course need to have been encoded for URL in the request.
    """
    try:
        dagruns = get_dag_runs(dag_id, execution_date=execution_date)
    except AirflowException as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = 404
        return response
    except ValueError:
        error_message = (
            'Given execution date, {}, could not be identified '
            'as a date. Example date format: 2015-11-16T14:34:15+00:00'.format(
                execution_date))
        _log.info(error_message)
        response = jsonify({'error': error_message})
        response.status_code = 400
        return response

    if not dagruns:
        error_message = "No Dag run found with provided execution date"
        response = jsonify(error=error_message)
        response.status_code = 404
        return response
    return jsonify(dagruns[0])


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
    return jsonify(items=payload)  # old flask versions dont support jsonifying arrays


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
