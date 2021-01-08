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
from airflow.api.common.experimental import pool as pool_api
from airflow.api.common.experimental import trigger_dag as trigger
from airflow.api.common.experimental.get_dag_runs import get_dag_runs
from airflow.api.common.experimental.get_task import get_task
from airflow.api.common.experimental.get_task_instance import get_task_instance
from airflow.api.common.experimental.get_code import get_code
from airflow.api.common.experimental.get_dag_run_state import get_dag_run_state
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.strings import to_boolean
from airflow.utils import timezone
from airflow.www_rbac.app import csrf
from airflow import models
from airflow.utils.db import create_session
from airflow.utils.curve import trigger_training_dag, get_curve_entity_ids, get_curve, trigger_push_result_to_mq, \
    do_save_curve_error_tag
from flask import g, Blueprint, jsonify, request, url_for
import json
from airflow.api.common.experimental.mark_tasks import modify_task_instance
import os
from airflow.utils.db import provide_session
from airflow.models import TaskInstance
import datetime
from random import choices
import pendulum
import math

_log = LoggingMixin().log

requires_authentication = airflow.api.API_AUTH.api_auth.requires_authentication

api_experimental = Blueprint('api_experimental', __name__)

ANALYSIS_NOK_RESULTS = True if os.environ.get('ANALYSIS_NOK_RESULTS', 'False') == 'True' else False

FILTER_MISMATCHES = True if os.environ.get('FILTER_MISMATCHES', 'False') == 'True' else False

MISMATCH_RATE_RELAXATION_FACTOR = float(os.environ.get('MISMATCH_RATE_RELAXATION_FACTOR', '1'))
MISMATCH_RATE_RELAXATION_THRESHOLD = float(os.environ.get('MISMATCH_RATE_RELAXATION_THRESHOLD', '0.001'))


def is_mismatch(measure_result, curve_mode):
    analysis_result = 'OK' if curve_mode is 0 else 'NOK'
    return analysis_result != measure_result


@provide_session
def get_recent_mismatch_rate(dag_id, task_id, session=None):
    delta = datetime.timedelta(days=2)
    min_date = timezone.utcnow() - delta
    total = session.query(TaskInstance).filter(
        TaskInstance.dag_id == dag_id,
        TaskInstance.task_id == task_id,
        TaskInstance.execution_date > min_date
    ).count()
    mismatches = session.query(TaskInstance).filter(
        TaskInstance.dag_id == dag_id,
        TaskInstance.task_id == task_id,
        TaskInstance.execution_date > min_date,
        TaskInstance.measure_result != TaskInstance.result
    ).count()
    _log.info('total:{},mismatches:{}'.format(total, mismatches))
    return mismatches / (total + 1), total


def mismatch_relaxation(mismatch_rate, count) -> bool:
    if mismatch_rate < MISMATCH_RATE_RELAXATION_THRESHOLD:
        return False
    weight = MISMATCH_RATE_RELAXATION_FACTOR * (mismatch_rate - MISMATCH_RATE_RELAXATION_THRESHOLD) / (
        mismatch_rate + MISMATCH_RATE_RELAXATION_THRESHOLD) * math.log(count, 2)
    _log.info('weight: {}'.format(weight))
    return choices([True, False], weights=[weight, 1])[0]


def filter_mismatches(measure_result, curve_mode, dag_id, task_id, execution_date):
    if not is_mismatch(measure_result, curve_mode):
        _log.info('not mismatch')
        return curve_mode
    _log.info('is mismatch')
    mismatch_rate, count = get_recent_mismatch_rate(dag_id, task_id)
    _log.info('mismatch_rate:{}, count:{}'.format(mismatch_rate, count))
    if mismatch_relaxation(mismatch_rate, count):
        return 0 if measure_result == 'OK' else 1
    return curve_mode


@csrf.exempt
@api_experimental.route('/taskinstance/analysis_result', methods=['PUT'])
@requires_authentication
def put_anaylysis_result():
    try:
        data = request.get_json(force=True)
        dag_id = data.get('dag_id')
        task_id = data.get('task_id')
        execution_date = data.get('exec_date')
        entity_id = data.get('entity_id')
        measure_result = data.get('measure_result')
        curve_mode = int(data.get('result'))  # OK, NOK

        if FILTER_MISMATCHES:
            curve_mode = filter_mismatches(measure_result, curve_mode, dag_id, task_id, execution_date)

        verify_error = int(data.get('verify_error'))  # OK, NOK
        rresult = 'OK' if curve_mode is 0 else 'NOK'

        if (not ANALYSIS_NOK_RESULTS) and measure_result == 'NOK':
            rresult = 'NOK'

        def modifier(ti):
            ti.result = rresult
            ti.measure_result = measure_result
            ti.entity_id = entity_id
            ti.error_code = curve_mode
            if curve_mode is not 0:
                ti.error_tag = json.dumps([curve_mode])
            else:
                ti.error_tag = json.dumps([])
            ti.verify_error = verify_error

        date = timezone.parse(execution_date)
        modify_task_instance(dag_id, task_id, execution_date=date, modifier=modifier)
        trigger_push_result_to_mq(
            'analysis_result',
            rresult,
            entity_id,
            execution_date,
            task_id,
            dag_id,
            verify_error,
            curve_mode
        )
        resp = jsonify({'response': 'ok'})
        resp.status_code = 200
        return resp
    except Exception as e:
        resp = jsonify({'error': repr(e)})
        resp.status_code = 500
        return resp


@csrf.exempt
@api_experimental.route('/dags/<string:dag_id>/tasks/<string:task_id>/<string:execution_date>/error_tag',
                        methods=['POST'])
@requires_authentication
def save_curve_error_tag(dag_id, task_id, execution_date):
    return _save_curve_error_tag(dag_id, task_id, execution_date)


@csrf.exempt
@api_experimental.route('/error_tag/dags/<string:dag_id>/tasks/<string:task_id>/<string:execution_date>',
                        methods=['POST'])
@requires_authentication
def save_curve_error_tag_w_csrf(dag_id, task_id, execution_date):
    return _save_curve_error_tag(dag_id, task_id, execution_date)


def _save_curve_error_tag(dag_id, task_id, execution_date):
    try:
        params = request.get_json(force=True)  # success failed
        error_tags = params.get('error_tags', [])
        do_save_curve_error_tag(dag_id, task_id, execution_date, error_tags)
        return jsonify(response='ok')
    except Exception as e:
        _log.info(repr(e))
        response = jsonify({'error': repr(e)})
        response.status_code = 400
        return response


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
        execution_date=dr.execution_date.isoformat(),
        run_id=dr.run_id
    )
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


@api_experimental.route(
    '/dags/<string:dag_id>/tasks/<string:task_id>/<string:execution_date>/confirm',
    methods=['POST'])
@requires_authentication
def double_confirm_task(dag_id, task_id, execution_date):
    try:
        date = timezone.parse(execution_date)
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
        error_tags = params.get('error_tags', [])

        task = get_task_instance(dag_id, task_id, execution_date=date)
        if not task.result:
            raise AirflowException(u"分析结果还没有生成，请等待分析结果生成后再进行二次确认")
        if not final_state or final_state not in ['OK', 'NOK']:
            raise AirflowException("二次确认参数未定义或数值不正确!")
        trigger_training_dag(dag_id, task_id, execution_date, final_state, error_tags)
        return jsonify({'response': 'ok'})
    except AirflowException as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response


@api_experimental.route(
    '/curve-entities',
    methods=['GET'])
@requires_authentication
def get_curves():
    try:
        craft_type = request.args.get('craft_type')
        bolt_number = request.args.get('bolt_number')
        entity_ids = get_curve_entity_ids(bolt_number, craft_type)
        return jsonify(entity_ids)
    except AirflowException as e:
        _log.info(e)
        response = jsonify(error="{}".format(e))
        response.status_code = e.status_code
        return response


@api_experimental.route(
    '/curves',
    methods=['GET'])
@requires_authentication
def get_curves_by_entity_id():
    try:
        curves = []

        vals = request.args.get('entity_ids')
        entity_ids = str(vals).split(",")
        if entity_ids is None:
            return jsonify(curves)

        for entity_id in entity_ids:
            try:
                curve = get_curve(entity_id)
                if curve is not None:
                    curves.append({
                        'entity_id': entity_id,
                        'curve': curve
                    })
            except Exception as e:
                _log.debug(e)
                curves.append({
                    'entity_id': entity_id,
                    'curve': []
                })

        return jsonify(curves=curves)
    except AirflowException as e:
        _log.info(e)
        response = jsonify(error="{}".format(repr(e)))
        response.status_code = e.status_code
        return response


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


@api_experimental.route('/dags/<string:dag_id>/paused', methods=['GET'])
@requires_authentication
def dag_is_paused(dag_id):
    """Get paused state of a dag"""

    is_paused = models.DagModel.get_dagmodel(dag_id).is_paused

    return jsonify({'is_paused': is_paused})


def do_remove_curve_from_curve_template(bolt_no=None, craft_type=None, version=None, mode=None, group_center_idx=None,
                                        curve_idx=None):
    if version is None or not bolt_no or not craft_type \
        or mode is None or group_center_idx is None or curve_idx is None:
        raise Exception('参数错误')
    template_name = '{}/{}'.format(bolt_no, craft_type)
    key, curve_template = Variable.get_fuzzy_active(template_name,
                                                    deserialize_json=True,
                                                    default_var=None
                                                    )
    template_version = curve_template.get('version', 0)
    if version != template_version:
        raise Exception('曲线模板信息过期，请刷新页面')
    if curve_template is None:
        raise Exception('读取模板信息失败')
    template_cluster = curve_template.get('template_cluster', None)
    if template_cluster is None:
        raise Exception('读取模板簇失败')
    mode_cluster = template_cluster.get(mode, None)
    if mode_cluster is None:
        raise Exception('无法找到对应模式的模板簇')
    groups = mode_cluster.get('curve_template_group_array', None)
    if groups is None:
        raise Exception('无法找到对应模式的曲线组')
    group = groups[group_center_idx]  # fixme
    if group is None:
        raise Exception('无法找到对应模式的曲线组')
    template_data_array = group.get('template_data_array', None)
    if group is None:
        raise Exception('无法找到对应模式的曲线组')
    if template_data_array[curve_idx]:
        del template_data_array[curve_idx]
    if len(template_data_array) == 0:
        del groups[group_center_idx]
        mode_cluster['curve_template_groups_k'] -= 1
    if mode_cluster['curve_template_groups_k'] == 0:
        del template_cluster[mode]
    curve_template.update({
        'version': template_version + 1
    })
    Variable.set(key, curve_template, serialize_json=True, is_curve_template=True)
    dag_id = 'load_all_curve_tmpls'
    conf = {
        'template_names': [template_name]
    }
    trigger.trigger_dag(dag_id, conf=conf, replace_microseconds=False)
    return curve_template


# @api_experimental.route('/curve_template/<string:bolt_no>/<string:craft_type>/remove_curve', methods=['PUT'])
# @requires_authentication
# def remove_curve_from_curve_template(bolt_no, craft_type):
#     params = request.get_json(force=True)
#     version = params.get('version', None)
#     mode = params.get('mode', None)
#     group_center_idx = params.get('group_center_idx', None)
#     curve_idx = params.get('curve_idx', None)
#     try:
#         new_template = do_remove_curve_from_curve_template(bolt_no, craft_type, version, mode, group_center_idx,
#                                                            curve_idx)
#         return {'data': new_template}
#     except Exception as e:
#         return {'error': str(e)}


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
