from plugins.result_storage.model import ResultModel
import json
from flask_babel import gettext
from plugins import AirflowModelView
from flask_babel import lazy_gettext
from airflow.plugins_manager import AirflowPlugin
from airflow.models.error_tag import ErrorTag
from airflow.configuration import conf

PAGE_SIZE = conf.getint('webserver', 'page_size')

class ResultModelView(AirflowModelView):
    route_base = '/results'

    datamodel = AirflowModelView.CustomSQLAInterface(ResultModel)

    base_permissions = ['can_list']

    page_size = PAGE_SIZE

    list_columns = ['entity_id', 'update_time', 'car_code', 'type', 'bolt_number', 'measure_result', 'result',
                    'final_state']

    search_columns = ['entity_id', 'update_time', 'car_code', 'type', 'bolt_number', 'measure_result', 'result',
                      'final_state']

    label_columns = {
        'entity_id': lazy_gettext('Entity Id'),
    }

    base_order = ('update_time', 'desc')

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


result_view = ResultModelView()
result_view_package = {"name": gettext("Results"),
                     "category": gettext("Analysis"),
                     "view": result_view}


class ResultViewPlugin(AirflowPlugin):
    name = "result_view"
    appbuilder_views = [result_view_package]
