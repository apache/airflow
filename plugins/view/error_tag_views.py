# -*- coding: utf-8 -*-

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
from plugins.models.error_tag import ErrorTag
from airflow.www_rbac.decorators import has_dag_access
from airflow.utils.log.custom_log import CUSTOM_LOG_FORMAT, CUSTOM_EVENT_NAME_MAP, CUSTOM_PAGE_NAME_MAP
import logging
import pprint

_logger = logging.getLogger(__name__)


class ErrorTagFilter(BaseFilter):

    def apply(self, query, func):  # noqa
        _logger.info("ErrorTagFilter: {}".format(pprint.pformat(func)))
        query, field = get_field_setup_query(query, self.model, self.column_name)
        return query


class ErrorTagForm(DynamicForm):
    value = StringField(
        lazy_gettext('Value'),
        widget=BS3TextFieldWidget())
    label = StringField(
        lazy_gettext('Label'),
        widget=BS3TextFieldWidget())


class ErrorTagModelView(AirflowModelView):
    route_base = '/error_tag'

    datamodel = AirflowModelView.CustomSQLAInterface(ErrorTag)

    base_permissions = ['can_add', 'can_list', 'can_edit', 'can_delete']

    extra_fields = []
    list_columns = ['value', 'label']
    add_columns = edit_columns = ['value', 'label'] + extra_fields
    add_form = edit_form = ErrorTagForm
    add_template = 'error_tag_create.html'
    edit_template = 'error_tag_edit.html'
    label_columns = {
        'value': lazy_gettext('Value'), 'label': lazy_gettext('Label')

    }
    base_order = ('id', 'asc')

    def post_add(self, item):
        super(ErrorTagModelView, self).post_add(item)
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['ADD'], CUSTOM_PAGE_NAME_MAP['ERROR_TAG'], '增加错误标签')
        logging.info(msg)

    def post_update(self, item):
        super(ErrorTagModelView, self).post_update(item)
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['UPDATE'], CUSTOM_PAGE_NAME_MAP['ERROR_TAG'], '修改错误标签')
        logging.info(msg)

    def post_delete(self, item):
        super(ErrorTagModelView, self).post_delete(item)
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['DELETE'], CUSTOM_PAGE_NAME_MAP['ERROR_TAG'], '删除错误标签')
        logging.info(msg)

    @action('export_analysis', "Export Statistics", '', single=False)
    @provide_session
    def action_export_error_tag_statistics(self, error_tags, session=None):
        ret = {}
        d = json.JSONDecoder()
        for var in error_tags:
            try:
                val = d.decode(var.val)
            except Exception:
                val = var.val
            ret[var.key] = val

        response = make_response(json.dumps(ret, sort_keys=True, indent=4, ensure_ascii=False))
        response.headers["Content-Disposition"] = "attachment; filename=错误标签分析.json"
        response.headers["Content-Type"] = "application/json; charset=utf-8"
        return response

    @action('muldelete', 'Delete', 'Are you sure you want to delete selected records?',
            single=False)
    @has_dag_access(can_dag_edit=True)
    def action_muldelete(self, items):
        self.datamodel.delete_all(items)
        self.update_redirect()
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['DELETE'], CUSTOM_PAGE_NAME_MAP['ERROR_TAG'], '删除选中错误标签')
        logging.info(msg)
        return redirect(self.get_redirect())

    # 重写list
    @expose("/list/")
    @has_access
    def list(self):
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['VIEW'], CUSTOM_PAGE_NAME_MAP['ERROR_TAG'], '查看错误标签')
        logging.info(msg)
        return super(ErrorTagModelView, self).list()


error_tag_view = ErrorTagModelView()
error_tag_package = {"name": gettext("Error Tags"),
                     "category": gettext("Master Data Management"),
                     "view": error_tag_view}


class ErrorTagViewPlugin(AirflowPlugin):
    name = "error_tag_view"
    appbuilder_views = [error_tag_package]
