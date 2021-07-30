from plugins.result_storage.model import ResultModel
import json
from flask_babel import gettext
from plugins import AirflowModelView
from datetime import datetime
from flask_login import current_user
from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
from wtforms.fields import StringField
from flask_appbuilder.forms import DynamicForm
from flask_babel import lazy_gettext
from flask_appbuilder.actions import action
from flask_appbuilder import expose, has_access
from flask import make_response, redirect
from airflow.plugins_manager import AirflowPlugin
from airflow.settings import TIMEZONE
from airflow.utils.db import provide_session
from flask_appbuilder.models.sqla.filters import BaseFilter, get_field_setup_query
from airflow.models.error_tag import ErrorTag
from airflow.www_rbac.decorators import has_dag_access
from airflow.utils.log.custom_log import CUSTOM_LOG_FORMAT, CUSTOM_EVENT_NAME_MAP, CUSTOM_PAGE_NAME_MAP
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
