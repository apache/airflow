from jinja2.utils import htmlsafe_json_dumps  # type: ignore
import numpy as np
from plugins.utils.utils import get_results
from airflow.utils.spc.lexen_spc.chart import covert2dArray, xbar_rbar, rbar, xbar_sbar, sbar, cpk
from airflow.utils.spc.lexen_spc.plot import histogram, normal
from plugins.utils.misc import profile, get_first_valid_data
from airflow.configuration import conf
import os
from flask import jsonify, request, Blueprint
from airflow.plugins_manager import AirflowPlugin
from airflow.www.app import csrf
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin

_log = LoggingMixin().log

PROFILE_DIR = conf.get('core', 'PROFILE_DIR')
SPC_SIZE = int(os.getenv('ENV_SPC_SIZE', '5'))

SPC_MIN_LEN = int(os.getenv('ENV_SPC_MIN_LEN', '25'))

TORQUE = "torque"
ANGLE = "angle"
CPK = "CPK"
CMK = "CMK"
mm = {TORQUE: 'measure_torque', ANGLE: 'measure_angle'}
mm_max = {TORQUE: 'torque_max', ANGLE: 'angle_max'}
mm_min = {TORQUE: 'torque_min', ANGLE: 'angle_min'}

bp = Blueprint('curve_spc', __name__)


# 根据传入的曲线entity_id获取spc数据
@bp.route('/spc', methods=['GET'])
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


# Defining the plugin class
class CurveSpcPlugin(AirflowPlugin):
    name = "curve_spc_plugin"
    flask_blueprints = [csrf.exempt(bp)]
