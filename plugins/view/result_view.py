from plugins.models.result import ResultModel
import json
from plugins import AirflowModelView
from flask_babel import lazy_gettext
from airflow.plugins_manager import AirflowPlugin
from plugins.models.error_tag import ErrorTag
from airflow.configuration import conf
from airflow.security import permissions
from flask_appbuilder import expose
from flask import redirect, abort, url_for

PAGE_SIZE = conf.getint('webserver', 'page_size')


class ResultModelView(AirflowModelView):
    route_base = '/results'

    datamodel = AirflowModelView.CustomSQLAInterface(ResultModel)

    page_size = PAGE_SIZE

    list_columns = ['entity_id', 'update_time', 'car_code', 'type', 'bolt_number', 'measure_result', 'result',
                    'final_state']

    search_columns = ['entity_id', 'update_time', 'car_code', 'type', 'bolt_number', 'measure_result', 'result',
                      'final_state']

    label_columns = {
        'entity_id': lazy_gettext('Entity Id'),
    }

    base_order = ('update_time', 'desc')

    base_permissions = [
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_ACCESS_MENU
    ]

    method_permission_name = {
        'list': 'read',
        'show': 'read',
        'edit': 'edit_all',
    }

    class_permission_name = permissions.RESOURCE_RESULT

    # base_filters = [['dag_id', DagFilter, lambda: []]]

    def error_tag_f(attr):
        ret = []
        try:
            error_tags = json.loads(attr.get('error_tag') or '[]')
            if not error_tags:
                return u'无异常标签'
            error_tag_vals = ErrorTag.get_all_dict() or {}
            for tag in error_tags:
                v = error_tag_vals.get(str(tag), '')
                if not v:
                    continue
                ret.append(v)
        except Exception as e:
            return ','.join(ret)
        return ','.join(ret)

    #
    def type_f(attr):
        ti_type = attr.get('type')
        if ti_type == 'rework':
            return lazy_gettext('Task Instance Rework')
        return lazy_gettext('Task Instance Normal')

    formatters_columns = {
        'error_tag': error_tag_f,
        'type': type_f,
    }

    @expose("/show/<pk>", methods=["GET"])
    def show(self, pk):
        pk = self._deserialize_pk_if_composite(pk)
        item = self.datamodel.get(pk, self._base_filters)
        if not item:
            abort(404)
        return redirect(url_for('CurveView.view_curve_page', entity_id=item.entity_id.replace('/', '@')))


result_view = ResultModelView()
result_view_package = {"name": permissions.RESOURCE_RESULT,
                       "category": permissions.RESOURCE_ANALYSIS,
                       "view": result_view}


class ResultViewPlugin(AirflowPlugin):
    name = "result_view"
    appbuilder_views = [result_view_package]
