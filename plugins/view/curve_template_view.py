from jinja2.utils import htmlsafe_json_dumps  # type: ignore
from airflow.plugins_manager import AirflowPlugin
from flask_login import current_user
from airflow.settings import TIMEZONE
from datetime import datetime
from flask import request
from flask_appbuilder import BaseView, expose, has_access
from jinja2.utils import htmlsafe_json_dumps  # type: ignore
from airflow.models import Variable
from airflow.exceptions import AirflowNotFoundException
from airflow.utils.log.custom_log import CUSTOM_LOG_FORMAT, CUSTOM_EVENT_NAME_MAP, CUSTOM_PAGE_NAME_MAP
import logging
from airflow.api.common.experimental import trigger_dag as trigger

_logger = logging.getLogger(__name__)


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


class CurveTemplateView(BaseView):
    route_base = ''

    @expose('/curve_template/<string:bolt_no>/<string:craft_type>')
    @has_access
    def view_curve_template(self, bolt_no, craft_type):
        curve_template = Variable.get_fuzzy_active('{}/{}'.format(bolt_no, craft_type),
                                                   deserialize_json=True,
                                                   default_var=None
                                                   )[1]
        _has_access = self.appbuilder.sm.has_access
        can_delete = _has_access('can_edit', 'VariableModelView') and _has_access('can_remove_curve_template',
                                                                                  'Airflow')

        if curve_template is None:
            # todo: 不要返回错误页面
            return AirflowNotFoundException()

        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['VIEW'], CUSTOM_PAGE_NAME_MAP['CURVE_TEMPLATE'],
                                       '查看曲线模板页面')
        logging.info(msg)
        device_type = 'tightening'  # fixme: 临时使用固定的device_type, 后续在控制器配置中添加
        if device_type == 'servo_press':
            cur_key_map = {
                'cur_w': '位移',
                'cur_m': '压力',
                'cur_t': '时间',
            }
        else:
            cur_key_map = {
                'cur_w': '角度',
                'cur_m': '扭矩',
                'cur_t': '时间',
                'cur_s': '转速'
            }
        return self.render_template('curve_template.html', can_delete=can_delete,
                                    curve_template=curve_template, bolt_no=bolt_no,
                                    craft_type=craft_type, device_type=device_type, cur_key_map=cur_key_map)

    @expose('/curve_template/<string:bolt_no>/<string:craft_type>/remove_curve', methods=['PUT'])
    @has_access
    def remove_curve_template(self, bolt_no, craft_type):
        _has_access = self.appbuilder.sm.has_access
        can_delete = _has_access('can_edit', 'VariableModelView')
        if not can_delete:
            return {'error': u'没有权限'}

        params = request.get_json(force=True)
        version = params.get('version', None)
        mode = params.get('mode', None)
        group_center_idx = params.get('group_center_idx', None)
        curve_idx = params.get('curve_idx', None)
        try:
            new_template = do_remove_curve_from_curve_template(bolt_no, craft_type, version, mode, group_center_idx,
                                                               curve_idx)
            msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                           current_user, getattr(current_user, 'last_name', ''),
                                           CUSTOM_EVENT_NAME_MAP['DELETE'],
                                           CUSTOM_PAGE_NAME_MAP['CURVE_TEMPLATE'], '删除曲线模板')
            logging.info(msg)
            return {'data': new_template}
        except Exception as e:
            return {'error': str(e)}


curve_template_view = CurveTemplateView()
curve_template_view_package = {"view": curve_template_view}


class CurveTemplateViewPlugin(AirflowPlugin):
    name = "curve_template_view_plugin"
    appbuilder_views = [curve_template_view_package]
