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
import numpy as np
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
from plugins.utils import get_results
from flask import g, Blueprint, jsonify, request, url_for
import os
from airflow.utils.spc.lexen_spc.chart import covert2dArray, xbar_rbar, rbar, xbar_sbar, sbar, cpk
from airflow.utils.spc.lexen_spc.plot import histogram, normal
from airflow.utils.misc import profile, get_first_valid_data
from airflow.configuration import conf

PROFILE_DIR = conf.get('core', 'PROFILE_DIR')

_log = LoggingMixin().log

requires_authentication = airflow.api.API_AUTH.api_auth.requires_authentication

api_experimental = Blueprint('api_experimental', __name__)

SPC_SIZE = int(os.getenv('ENV_SPC_SIZE', '5'))

SPC_MIN_LEN = int(os.getenv('ENV_SPC_MIN_LEN', '25'))


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
        dr = trigger.trigger_dag(dag_id, run_id, conf, execution_date, replace_microseconds)
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


@api_experimental.route('/dags/<string:dag_id>/tasks/<string:task_id>', methods=['GET'])
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


TORQUE = "torque"
ANGLE = "angle"
CPK = "CPK"
CMK = "CMK"
mm = {TORQUE: 'measure_torque', ANGLE: 'measure_angle'}
mm_max = {TORQUE: 'torque_max', ANGLE: 'angle_max'}
mm_min = {TORQUE: 'torque_min', ANGLE: 'angle_min'}


# 根据传入的曲线entity_id获取spc数据
@api_experimental.route('/spc', methods=['GET'])
@requires_authentication
@profile(os.path.join(PROFILE_DIR, 'spc.profile'))
def get_spc_by_entity_id():
    spc = {'x-r': {"title": u"Xbar-R 控制图", "data": {TORQUE: {}, ANGLE: {}}},
           'x-s': {"title": u"Xbar-S 控制图", "data": {TORQUE: {}, ANGLE: {}}},
           'n-d': {"title": u"正态分布 图", "data": {TORQUE: {}, ANGLE: {}}},
           }
    x_r_entry = spc.get('x-r').get('data')
    x_s_entry = spc.get('x-s').get('data')
    n_d_entry = spc.get('n-d').get('data')
    try:
        vals = request.args.get('entity_ids')
        tType = request.args.get('type', 'torque')  # 默认是扭矩图
        spc.update({'type': tType})
        entity_ids = str(vals).split(",")
        if not entity_ids:
            raise AirflowException(u'entityID为空!')
        results = get_results(entity_ids)
        ll = len(results)
        if not results or ll == 0:
            raise AirflowException(u'未找到结果!')
        if ll < SPC_MIN_LEN:
            raise AirflowException(u'数据长度: {} 小于设定的SPC最小数据长度: {}!'.format(ll, SPC_MIN_LEN))
        origin_data = {TORQUE: [], ANGLE: []}
        for key in origin_data.keys():
            entry = origin_data.get(key)
            for result in results:
                entry.append(result.get(mm.get(key), None))
            if not all(entry):
                raise AirflowException(u'')
            data = covert2dArray(entry, SPC_SIZE)
            if data is None:
                raise AirflowException(u'SPC 数据格式不正确!')
            xr_xbar_part = xbar_rbar(data, SPC_SIZE)
            xr_r_part = rbar(data, SPC_SIZE)
            xs_xbar_part = xbar_sbar(data, SPC_SIZE, None)
            xs_s_part = sbar(data, SPC_SIZE, None)
            cpk_data = cpk(entry, get_first_valid_data(results, mm_max.get(key)),
                           get_first_valid_data(results, mm_min.get(key)))
            # todo: SPC包
            # xbar-r chart
            xr_ee: dict = x_r_entry.get(key)
            if xr_ee is None:
                _log.error(u"未找到入口: {}".format(key))
                continue
            xr_ee.update({
                'xbar': xr_xbar_part,
                'r': xr_r_part,
                'cpk': cpk_data
            })
            # xbar-s chart
            xs_ee: dict = x_s_entry.get(key)
            if xs_ee is None:
                _log.error(u"未找到入口: {}".format(key))
                continue
            xs_ee.update({
                'xbar': xs_xbar_part,
                's': xs_s_part,
                'cpk': cpk_data
            })

        # 正态分布图
        nd_data = origin_data.get(tType)
        if len(nd_data) > 0:
            first_result = results[0]
            # 正常是使用target值的正负百分之三
            # _target = first_result.get(tType+"_target")
            # if not _target:
            #     _target = (first_result.get(tType+"_min") + first_result.get(tType+"_max")) / 2
            # usl = _target * (1+0.03)
            # lsl = _target * (1-0.03)
            usl = first_result.get(tType + "_max")
            lsl = first_result.get(tType + "_min")
            spc_step = 1
            his = histogram(nd_data, usl, lsl, spc_step)
            nor = normal(nd_data, usl, lsl, spc_step)
            x1, y1, y2 = [], [], []
            for i, val in enumerate(his[0]):
                if i + 1 < len(his[0]):
                    x1.append('(%.1f,%.1f)' % (val, his[0][i + 1]))
            for i, val in enumerate(his[1]):
                if np.isnan(val):
                    val = 0
                y1.append(round(val * 100, 2))
            for i, val in enumerate(nor[1]):
                if np.isnan(val):
                    val = 0
                y2.append(round(val * 100, 2))
            n_d_entry.get(tType).update(
                {
                    'x1': x1,
                    'y1': y1,
                    'y2': y2
                }
            )
        _log.info(spc)
        return jsonify(spc=spc)
    except AirflowException as e:
        _log.error("get_spc_by_entity_id", e)
        response = jsonify(error=str(e))
        response.status_code = e.status_code
        return response
    except BaseException as e:
        _log.error("get_spc_by_entity_id", e)
        response = jsonify(error=str(e))
        response.status_code = 500
        return response


# ToDo: Shouldn't this be a PUT method?
@api_experimental.route('/dags/<string:dag_id>/paused/<string:paused>', methods=['GET'])
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
