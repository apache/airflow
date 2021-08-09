import json

from jinja2.utils import htmlsafe_json_dumps  # type: ignore
from airflow.plugins_manager import AirflowPlugin
from flask_login import current_user
from airflow.settings import TIMEZONE
from datetime import datetime
from flask_appbuilder import expose, has_access
from jinja2.utils import htmlsafe_json_dumps  # type: ignore
from airflow.exceptions import AirflowNotFoundException
from airflow.utils.log.custom_log import CUSTOM_LOG_FORMAT, CUSTOM_EVENT_NAME_MAP, CUSTOM_PAGE_NAME_MAP
import logging
from airflow.api.common.experimental import trigger_dag as trigger
from plugins import AirflowModelView
from plugins.models.curve_template import CurveTemplateModel
import uuid
from flask_appbuilder.actions import action
from airflow.www_rbac.decorators import action_logging
from flask_babel import gettext
from flask._compat import PY2
from flask import (
    Markup, flash, make_response, redirect, request
)

_logger = logging.getLogger(__name__)


def do_remove_curve_from_curve_template(bolt_no=None, craft_type=None, version=None, mode=None, group_center_idx=None,
                                        curve_idx=None):
    if version is None or not bolt_no or not craft_type \
        or mode is None or group_center_idx is None or curve_idx is None:
        raise Exception('参数错误')
    template_name = '{}/{}'.format(bolt_no, craft_type)
    key, curve_template = CurveTemplateModel.get_fuzzy_active(template_name,
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
    CurveTemplateModel.set(key, curve_template, serialize_json=True)
    dag_id = 'load_all_curve_tmpls'
    conf = {
        'template_names': [template_name]
    }
    trigger.trigger_dag(dag_id, conf=conf, replace_microseconds=False)
    return curve_template


class CurveTemplateView(AirflowModelView):
    route_base = '/curve_template'
    datamodel = AirflowModelView.CustomSQLAInterface(CurveTemplateModel)
    list_template = 'curve_template_list.html'
    edit_template = 'curve_template_edit.html'

    list_columns = ['key', 'val', 'active']
    add_columns = ['key', 'val', 'active']
    edit_columns = ['key', 'val', 'active']
    search_columns = ['key', 'val', 'active']

    label_columns = {
        'key': gettext('Key'),
        'val': gettext('Val'),
        'active': gettext('Active')
    }

    def hidden_field_formatter(attr):
        if isinstance(attr, str):
            return attr
        key = attr.get('key')
        val = attr.get('val')
        if val:
            return val
        else:
            return Markup('<span class="label label-danger">Invalid</span>')

    formatters_columns = {
        'val': hidden_field_formatter,
    }

    @expose('/<string:bolt_no>/<string:craft_type>')
    @has_access
    def view_curve_template(self, bolt_no, craft_type):
        curve_template = CurveTemplateModel.get_fuzzy_active('{}/{}'.format(bolt_no, craft_type),
                                                         deserialize_json=True,
                                                         default_var=None
                                                         )[1]
        _has_access = self.appbuilder.sm.has_access
        can_delete = _has_access('can_edit', 'CurveTemplateView') and _has_access('can_remove_curve_template',
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

    @expose('/<string:bolt_no>/<string:craft_type>/remove_curve', methods=['PUT'])
    @has_access
    def remove_curve_template(self, bolt_no, craft_type):
        _has_access = self.appbuilder.sm.has_access
        can_delete = _has_access('can_edit', 'CurveTemplateView')
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

    @expose("/list/")
    @has_access
    def list(self):
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['VIEW'], CUSTOM_PAGE_NAME_MAP['TIGHTENING_CURVE_TEMPLATE'],
                                       '曲线模板：查看列表')
        logging.info(msg)
        return super(CurveTemplateView, self).list()

    @staticmethod
    def generateCurveParamKey(key):
        return "{}@@{}".format(key, uuid.uuid3(uuid.NAMESPACE_DNS, key))

    @staticmethod
    def is_curve_param_key(key: str) -> bool:
        return '@@' in key

    def pre_add(self, item):
        super(CurveTemplateView, self).pre_add(item)
        item.key = self.generateCurveParamKey(item.key)

    def post_add(self, item):
        super(CurveTemplateView, self).post_add(item)
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['ADD'], CUSTOM_PAGE_NAME_MAP['TIGHTENING_CURVE_TEMPLATE'],
                                       '曲线模板：增加')
        logging.info(msg)

    def pre_update(self, item: CurveTemplateModel):
        super(CurveTemplateView, self).pre_update(item)
        if self.is_curve_param_key(item.key):
            return
        item.key = self.generateCurveParamKey(item.key)

    def post_update(self, item):
        super(CurveTemplateView, self).post_update(item)
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['UPDATE'],
                                       CUSTOM_PAGE_NAME_MAP['TIGHTENING_CURVE_TEMPLATE'], '曲线模板：修改')
        logging.info(msg)

    @action('muldelete', 'Delete', 'Are you sure you want to delete selected records?',
            single=False)
    def action_muldelete(self, items):
        self.datamodel.delete_all(items)
        self.update_redirect()
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['DELETE'],
                                       CUSTOM_PAGE_NAME_MAP['TIGHTENING_CURVE_TEMPLATE'], '曲线模板：删除选中变量')
        logging.info(msg)
        return redirect(self.get_redirect())

    @action('varexport', 'Export', '', single=False)
    def action_varexport(self, items):
        var_dict = {}
        d = json.JSONDecoder()
        for var in items:
            try:
                val = d.decode(var.val)
            except Exception:
                val = var.val
            var_dict[var.key] = val

        response = make_response(json.dumps(var_dict, sort_keys=True, indent=4, ensure_ascii=False))
        response.headers["Content-Disposition"] = "attachment; filename=curve_templates.json"
        response.headers["Content-Type"] = "application/json; charset=utf-8"
        return response

    @expose('/varimport', methods=["POST"])
    @has_access
    @action_logging
    def varimport(self):
        try:
            out = request.files['file'].read()
            if not PY2 and isinstance(out, bytes):
                d = json.loads(out.decode('utf-8'))
            else:
                d = json.loads(out)
        except Exception:
            self.update_redirect()
            flash("Missing file or syntax error.", 'error')
            return redirect(self.get_redirect())
        else:
            suc_count = fail_count = 0
            for k, v in d.items():
                try:
                    CurveTemplateModel.set(k, v, serialize_json=isinstance(v, dict))
                except Exception as e:
                    logging.info('Curve Template import failed: {}'.format(repr(e)))
                    fail_count += 1
                else:
                    suc_count += 1
            flash("{} Curve Template(s) successfully updated.".format(suc_count))
            if fail_count:
                flash("{} Curve Template(s) failed to be updated.".format(fail_count), 'error')
            self.update_redirect()
            return redirect(self.get_redirect())


curve_template_view = CurveTemplateView()
curve_template_view_package = {
    "name": gettext("Curve Template"),
    "category": gettext("Master Data Management"),
    "view": curve_template_view
}


class CurveTemplateViewPlugin(AirflowPlugin):
    name = "curve_template_view_plugin"
    appbuilder_views = [curve_template_view_package]
