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
#
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json
from datetime import datetime as dt
from operator import itemgetter

import pendulum
from flask_appbuilder.fieldwidgets import (
    BS3PasswordFieldWidget, BS3TextAreaFieldWidget, BS3TextFieldWidget, Select2Widget,
)
from flask_appbuilder.forms import DynamicForm
from flask_babel import lazy_gettext
from flask_wtf import FlaskForm
from wtforms import validators, widgets
from wtforms.fields import (
    BooleanField, Field, IntegerField, PasswordField, SelectField, StringField, TextAreaField,
)

from airflow.configuration import conf
from airflow.models import Connection
from airflow.utils import timezone
from airflow.www_rbac.validators import ValidJson
from airflow.www_rbac.widgets import AirflowDateTimePickerWidget


class DateTimeWithTimezoneField(Field):
    """
    A text field which stores a `datetime.datetime` matching a format.
    """
    widget = widgets.TextInput()

    def __init__(self, label=None, validators=None, format=None, **kwargs):
        super(DateTimeWithTimezoneField, self).__init__(label, validators, **kwargs)
        self.format = format or "%Y-%m-%d %H:%M:%S%Z"

    def _value(self):
        if self.raw_data:
            return ' '.join(self.raw_data)
        else:
            return self.data and self.data.strftime(self.format) or ''

    def process_formdata(self, valuelist):
        if valuelist:
            date_str = ' '.join(valuelist)
            try:
                # Check if the datetime string is in the format without timezone, if so convert it to the
                # default timezone
                if len(date_str) == 19:
                    parsed_datetime = dt.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                    defualt_timezone = pendulum.timezone('UTC')
                    tz = conf.get("core", "default_timezone")
                    if tz == "system":
                        defualt_timezone = pendulum.local_timezone()
                    else:
                        defualt_timezone = pendulum.timezone(tz)
                    self.data = defualt_timezone.convert(parsed_datetime)
                else:
                    self.data = pendulum.parse(date_str)
            except ValueError:
                self.data = None
                raise ValueError(self.gettext('Not a valid datetime value'))


class DateTimeForm(FlaskForm):
    # Date filter form needed for task views
    execution_date = DateTimeWithTimezoneField(
        "Execution date", widget=AirflowDateTimePickerWidget())


class DateTimeWithNumRunsForm(FlaskForm):
    # Date time and number of runs form for tree view, task duration
    # and landing times
    base_date = DateTimeWithTimezoneField(
        "Anchor date", widget=AirflowDateTimePickerWidget(), default=timezone.utcnow())
    num_runs = SelectField("Number of runs", default=25, choices=(
        (5, "5"),
        (25, "25"),
        (50, "50"),
        (100, "100"),
        (365, "365"),
    ))


class DateTimeWithNumRunsWithDagRunsForm(DateTimeWithNumRunsForm):
    # Date time and number of runs and dag runs form for graph and gantt view
    execution_date = SelectField("DAG run")


class DagRunForm(DynamicForm):
    dag_id = StringField(
        lazy_gettext('Dag Id'),
        validators=[validators.DataRequired()],
        widget=BS3TextFieldWidget())
    start_date = DateTimeWithTimezoneField(
        lazy_gettext('Start Date'),
        widget=AirflowDateTimePickerWidget())
    end_date = DateTimeWithTimezoneField(
        lazy_gettext('End Date'),
        widget=AirflowDateTimePickerWidget())
    run_id = StringField(
        lazy_gettext('Run Id'),
        validators=[validators.DataRequired()],
        widget=BS3TextFieldWidget())
    state = SelectField(
        lazy_gettext('State'),
        choices=(('success', 'success'), ('running', 'running'), ('failed', 'failed'),),
        widget=Select2Widget())
    execution_date = DateTimeWithTimezoneField(
        lazy_gettext('Execution Date'),
        widget=AirflowDateTimePickerWidget())
    external_trigger = BooleanField(
        lazy_gettext('External Trigger'))
    conf = TextAreaField(
        lazy_gettext('Conf'),
        validators=[ValidJson(), validators.Optional()],
        widget=BS3TextAreaFieldWidget())

    def populate_obj(self, item):
        super(DagRunForm, self).populate_obj(item)
        if item.conf:
            item.conf = json.loads(item.conf)


class ConnectionForm(DynamicForm):
    conn_id = StringField(
        lazy_gettext('Conn Id'),
        validators=[validators.DataRequired()],
        widget=BS3TextFieldWidget())
    conn_type = SelectField(
        lazy_gettext('Conn Type'),
        choices=sorted(Connection._types, key=itemgetter(1)),
        widget=Select2Widget())
    host = StringField(
        lazy_gettext('Host'),
        widget=BS3TextFieldWidget())
    schema = StringField(
        lazy_gettext('Schema'),
        widget=BS3TextFieldWidget())
    login = StringField(
        lazy_gettext('Login'),
        widget=BS3TextFieldWidget())
    password = PasswordField(
        lazy_gettext('Password'),
        widget=BS3PasswordFieldWidget())
    port = IntegerField(
        lazy_gettext('Port'),
        validators=[validators.Optional()],
        widget=BS3TextFieldWidget())
    extra = TextAreaField(
        lazy_gettext('Extra'),
        widget=BS3TextAreaFieldWidget())

    # Used to customized the form, the forms elements get rendered
    # and results are stored in the extra field as json. All of these
    # need to be prefixed with extra__ and then the conn_type ___ as in
    # extra__{conn_type}__name. You can also hide form elements and rename
    # others from the connection_form.js file
    extra__jdbc__drv_path = StringField(
        lazy_gettext('Driver Path'),
        widget=BS3TextFieldWidget())
    extra__jdbc__drv_clsname = StringField(
        lazy_gettext('Driver Class'),
        widget=BS3TextFieldWidget())
    extra__google_cloud_platform__project = StringField(
        lazy_gettext('Project Id'),
        widget=BS3TextFieldWidget())
    extra__google_cloud_platform__key_path = StringField(
        lazy_gettext('Keyfile Path'),
        widget=BS3TextFieldWidget())
    extra__google_cloud_platform__keyfile_dict = PasswordField(
        lazy_gettext('Keyfile JSON'),
        widget=BS3PasswordFieldWidget())
    extra__google_cloud_platform__scope = StringField(
        lazy_gettext('Scopes (comma separated)'),
        widget=BS3TextFieldWidget())
    extra__google_cloud_platform__num_retries = IntegerField(
        lazy_gettext('Number of Retries'),
        validators=[validators.NumberRange(min=0)],
        widget=BS3TextFieldWidget(),
        default=5)
    extra__grpc__auth_type = StringField(
        lazy_gettext('Grpc Auth Type'),
        widget=BS3TextFieldWidget())
    extra__grpc__credential_pem_file = StringField(
        lazy_gettext('Credential Keyfile Path'),
        widget=BS3TextFieldWidget())
    extra__grpc__scopes = StringField(
        lazy_gettext('Scopes (comma separated)'),
        widget=BS3TextFieldWidget())
