# -*- coding: utf-8 -*-
from flask import flash, make_response, request
import json
from flask_babel import lazy_gettext, gettext
from datetime import datetime
from flask_login import current_user
from flask_appbuilder.actions import action
from flask_appbuilder import expose
from flask import redirect
from plugins.common import AirflowModelView
from airflow.plugins_manager import AirflowPlugin
from airflow.settings import TIMEZONE
from airflow.www.decorators import action_logging
from flask_wtf.csrf import CSRFProtect
from plugins.utils.custom_log import CUSTOM_LOG_FORMAT, CUSTOM_EVENT_NAME_MAP, CUSTOM_PAGE_NAME_MAP
import logging
import os
import pandas as pd
from wtforms.ext.sqlalchemy.fields import QuerySelectField
from flask import current_app
from wtforms.fields import StringField
from flask_appbuilder.fieldwidgets import (
    BS3TextFieldWidget, Select2Widget,
)
from flask_appbuilder.forms import DynamicForm
from airflow.security import permissions
from airflow.www.widgets import AirflowModelListWidget

FACTORY_CODE = os.getenv('FACTORY_CODE', 'DEFAULT_FACTORY_CODE')

_logger = logging.getLogger(__name__)
csrf = CSRFProtect()


def device_type_query():
    print(current_app)
    session = current_app.appbuilder.get_session()
    from plugins.models.device_type import DeviceTypeModel
    return session.query(DeviceTypeModel)


def _get_related_pk_func(obj):
    return obj.id


class TighteningControllerForm(DynamicForm):
    controller_name = StringField(
        lazy_gettext('Equipment Name'),
        widget=BS3TextFieldWidget())
    line_code = StringField(
        lazy_gettext('Line Code'),
        widget=BS3TextFieldWidget())
    line_name = StringField(
        lazy_gettext('Line Name'),
        widget=BS3TextFieldWidget())
    work_center_code = StringField(
        lazy_gettext('Work Center Code'),
        widget=BS3TextFieldWidget())
    work_center_name = StringField(
        lazy_gettext('Work Center Name'),
        widget=BS3TextFieldWidget())

    device_type = QuerySelectField(
        lazy_gettext('Device Type'),
        query_factory=device_type_query,
        # get_pk_func=_get_related_pk_func,
        widget=Select2Widget(extra_classes="readonly")
    )


class TighteningControllerListWidget(AirflowModelListWidget):
    template = 'tightening_controller_list_widget.html'


class TighteningControllerView(AirflowModelView):
    route_base = '/tightening_controller'
    list_widget = TighteningControllerListWidget
    from plugins.models.tightening_controller import TighteningController
    datamodel = AirflowModelView.CustomSQLAInterface(TighteningController)

    extra_fields = []
    list_columns = ['controller_name', 'line_code', 'line_name', 'work_center_code', 'work_center_name', 'device_type']
    add_columns = edit_columns = ['controller_name', 'line_code', 'line_name', 'work_center_code',
                                  'work_center_name', 'device_type'] + extra_fields
    add_form = edit_form = TighteningControllerForm
    add_template = 'tightening_controller_create.html'
    edit_template = 'tightening_controller_edit.html'
    list_template = 'tightening_controller_list.html'
    label_columns = {
        'controller_name': lazy_gettext('Controller Name'),
        'line_code': lazy_gettext('Line Code'),
        'line_name': lazy_gettext('Line Name'),
        'work_center_code': lazy_gettext('Work Center Code'),
        'work_center_name': lazy_gettext('Work Center Name'),
        'device_type_id': lazy_gettext('Device Type'),
    }

    method_permission_name = {
        'list': 'read',
        'show': 'read',
        'add': 'create',
        'action_muldelete': 'delete',
        'controllerimport': 'create',
        'action_controllerexport': 'read'
    }

    class_permission_name = permissions.RESOURCE_CONTROLLER

    base_permissions = [
        permissions.ACTION_CAN_CREATE,
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_DELETE,
        permissions.ACTION_CAN_ACCESS_MENU
    ]

    base_order = ('id', 'asc')

    def post_add(self, item):
        super(TighteningControllerView, self).post_add(item)
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['ADD'], CUSTOM_PAGE_NAME_MAP['TIGHTENING_CONTROLLER'],
                                       '增加控制器')
        logging.info(msg)

    def post_update(self, item):
        super(TighteningControllerView, self).post_update(item)
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['UPDATE'], CUSTOM_PAGE_NAME_MAP['TIGHTENING_CONTROLLER'],
                                       '修改控制器')
        logging.info(msg)

    def post_delete(self, item):
        super(TighteningControllerView, self).post_delete(item)
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['DELETE'], CUSTOM_PAGE_NAME_MAP['TIGHTENING_CONTROLLER'],
                                       '删除控制器')
        logging.info(msg)

    @action('muldelete', 'Delete', 'Are you sure you want to delete selected records?',
            single=False)
    def action_muldelete(self, items):
        self.datamodel.delete_all(items)
        self.update_redirect()
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['DELETE'], CUSTOM_PAGE_NAME_MAP['TIGHTENING_CONTROLLER'],
                                       '删除选中控制器')
        logging.info(msg)
        return redirect(self.get_redirect())

    @expose('/controllerimport', methods=["POST"])
    @action_logging
    def controllerimport(self):
        try:
            d = pd.read_csv(request.files['file'])
            if d.empty:
                raise Exception('设备清单为空')
            data = d.to_json(orient='records')
            d = json.loads(data)
        except Exception as e:
            self.update_redirect()
            flash(repr(e), 'error')
            return redirect(self.get_redirect())
        suc_count = fail_count = 0
        controller: dict
        for controller in d:
            try:
                from plugins.models.tightening_controller import TighteningController
                TighteningController.add_controller(**controller)
            except Exception as e:
                logging.info('Controller import failed: {}'.format(repr(e)))
                fail_count += 1
            else:
                suc_count += 1
        flash("{} controller(s) successfully updated.".format(suc_count))
        if fail_count:
            flash("{} controller(s) failed to be updated.".format(fail_count), 'error')
        self.update_redirect()
        return redirect(self.get_redirect())

    @action('controllerexport', 'Export', '', single=False)
    def action_controllerexport(self, items):
        ret = []
        for controller in items:
            try:
                val = controller.as_dict()
            except Exception as e:
                val = str(controller)
            ret.append(val)
        d = pd.DataFrame.from_records(ret, index=[i for i in range(len(ret))])
        response = make_response(d.to_csv(index=False))
        response.headers["Content-Disposition"] = "attachment; filename=controllers.csv"
        response.headers["Content-Type"] = "application/json; charset=utf-8"
        return response

    @expose("/list/")
    def list(self):
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['VIEW'], CUSTOM_PAGE_NAME_MAP['TIGHTENING_CONTROLLER'],
                                       '查看控制器')
        logging.info(msg)
        _has_access = self.appbuilder.sm.has_access
        can_import = _has_access(permissions.ACTION_CAN_CREATE, permissions.RESOURCE_CONTROLLER)
        widgets = self._list()
        return self.render_template(
            self.list_template, title=self.list_title, widgets=widgets, can_import=can_import
        )


class DeviceTypeView(AirflowModelView):
    from plugins.models.device_type import DeviceTypeModel
    datamodel = AirflowModelView.CustomSQLAInterface(DeviceTypeModel)

    method_permission_name = {
        'list': 'read',
        'show': 'read',
        'add': 'create',
    }

    class_permission_name = permissions.RESOURCE_DEVICE_TYPE

    base_permissions = [
        permissions.ACTION_CAN_CREATE,
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_DELETE,
        permissions.ACTION_CAN_ACCESS_MENU
    ]


tightening_controller_view = TighteningControllerView()
tightening_controller_package = {"name": permissions.RESOURCE_CONTROLLER,
                                 "category": permissions.RESOURCE_MASTER_DATA_MANAGEMENT,
                                 "view": tightening_controller_view}

device_type_view = DeviceTypeView()
device_type_package = {"name": permissions.RESOURCE_DEVICE_TYPE,
                       "category": permissions.RESOURCE_MASTER_DATA_MANAGEMENT,
                       "view": device_type_view}


class TighteningControllerViewPlugin(AirflowPlugin):
    name = "tightening_controller_view"

    appbuilder_views = [tightening_controller_package, device_type_package]
