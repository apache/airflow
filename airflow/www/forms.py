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

import json
from datetime import datetime as dt

import pendulum
from flask_appbuilder.fieldwidgets import (
    BS3PasswordFieldWidget,
    BS3TextAreaFieldWidget,
    BS3TextFieldWidget,
    Select2Widget,
)
from flask_appbuilder.forms import DynamicForm
from flask_babel import lazy_gettext
from flask_wtf import FlaskForm
from wtforms import widgets
from wtforms.fields import (
    BooleanField,
    Field,
    IntegerField,
    PasswordField,
    SelectField,
    StringField,
    TextAreaField,
)
from wtforms.validators import InputRequired, Optional

from airflow.configuration import conf
from airflow.utils import timezone
from airflow.utils.types import DagRunType
from airflow.www.validators import ValidJson
from airflow.www.widgets import (
    AirflowDateTimePickerROWidget,
    AirflowDateTimePickerWidget,
    BS3TextAreaROWidget,
    BS3TextFieldROWidget,
)


class DateTimeWithTimezoneField(Field):
    """A text field which stores a `datetime.datetime` matching a format."""

    widget = widgets.TextInput()

    def __init__(self, label=None, validators=None, datetime_format='%Y-%m-%d %H:%M:%S%Z', **kwargs):
        super().__init__(label, validators, **kwargs)
        self.format = datetime_format
        self.data = None

    def _value(self):
        if self.raw_data:
            return ' '.join(self.raw_data)
        if self.data:
            return self.data.strftime(self.format)
        return ''

    def process_formdata(self, valuelist):
        if not valuelist:
            return
        date_str = ' '.join(valuelist)
        try:
            # Check if the datetime string is in the format without timezone, if so convert it to the
            # default timezone
            if len(date_str) == 19:
                parsed_datetime = dt.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                default_timezone = self._get_default_timezone()
                self.data = default_timezone.convert(parsed_datetime)
            else:
                self.data = pendulum.parse(date_str)
        except ValueError:
            self.data = None
            raise ValueError(self.gettext('Not a valid datetime value'))

    def _get_default_timezone(self):
        current_timezone = conf.get("core", "default_timezone")
        if current_timezone == "system":
            default_timezone = pendulum.local_timezone()
        else:
            default_timezone = pendulum.timezone(current_timezone)
        return default_timezone


class DateTimeForm(FlaskForm):
    """Date filter form needed for task views"""

    execution_date = DateTimeWithTimezoneField("Execution date", widget=AirflowDateTimePickerWidget())


class DateTimeWithNumRunsForm(FlaskForm):
    """
    Date time and number of runs form for tree view, task duration
    and landing times
    """

    base_date = DateTimeWithTimezoneField(
        "Anchor date", widget=AirflowDateTimePickerWidget(), default=timezone.utcnow()
    )
    num_runs = SelectField(
        "Number of runs",
        default=25,
        choices=(
            (5, "5"),
            (25, "25"),
            (50, "50"),
            (100, "100"),
            (365, "365"),
        ),
    )


class DateTimeWithNumRunsWithDagRunsForm(DateTimeWithNumRunsForm):
    """Date time and number of runs and dag runs form for graph and gantt view"""

    execution_date = SelectField("DAG run")


class DagRunForm(DynamicForm):
    """Form for adding DAG Run"""

    dag_id = StringField(lazy_gettext('Dag Id'), validators=[InputRequired()], widget=BS3TextFieldWidget())
    start_date = DateTimeWithTimezoneField(lazy_gettext('Start Date'), widget=AirflowDateTimePickerWidget())
    end_date = DateTimeWithTimezoneField(lazy_gettext('End Date'), widget=AirflowDateTimePickerWidget())
    run_id = StringField(lazy_gettext('Run Id'), validators=[InputRequired()], widget=BS3TextFieldWidget())
    state = SelectField(
        lazy_gettext('State'),
        choices=(
            ('success', 'success'),
            ('running', 'running'),
            ('failed', 'failed'),
        ),
        widget=Select2Widget(),
        validators=[InputRequired()],
    )
    execution_date = DateTimeWithTimezoneField(
        lazy_gettext('Execution Date'),
        widget=AirflowDateTimePickerWidget(),
        validators=[InputRequired()],
    )
    external_trigger = BooleanField(lazy_gettext('External Trigger'))
    conf = TextAreaField(
        lazy_gettext('Conf'), validators=[ValidJson(), Optional()], widget=BS3TextAreaFieldWidget()
    )

    def populate_obj(self, item):
        """Populates the attributes of the passed obj with data from the form’s fields."""
        super().populate_obj(item)
        item.run_type = DagRunType.from_run_id(item.run_id)
        if item.conf:
            item.conf = json.loads(item.conf)


class DagRunEditForm(DagRunForm):
    """Form for editing DAG Run"""

    dag_id = StringField(lazy_gettext('Dag Id'), validators=[InputRequired()], widget=BS3TextFieldROWidget())
    start_date = DateTimeWithTimezoneField(lazy_gettext('Start Date'), widget=AirflowDateTimePickerROWidget())
    end_date = DateTimeWithTimezoneField(lazy_gettext('End Date'), widget=AirflowDateTimePickerROWidget())
    run_id = StringField(lazy_gettext('Run Id'), validators=[InputRequired()], widget=BS3TextFieldROWidget())
    execution_date = DateTimeWithTimezoneField(
        lazy_gettext('Execution Date'),
        widget=AirflowDateTimePickerROWidget(),
        validators=[InputRequired()],
    )
    conf = TextAreaField(
        lazy_gettext('Conf'), validators=[ValidJson(), Optional()], widget=BS3TextAreaROWidget()
    )


class TaskInstanceEditForm(DynamicForm):
    """Form for editing TaskInstance"""

    dag_id = StringField(lazy_gettext('Dag Id'), validators=[InputRequired()], widget=BS3TextFieldROWidget())
    task_id = StringField(
        lazy_gettext('Task Id'), validators=[InputRequired()], widget=BS3TextFieldROWidget()
    )
    start_date = DateTimeWithTimezoneField(lazy_gettext('Start Date'), widget=AirflowDateTimePickerROWidget())
    end_date = DateTimeWithTimezoneField(lazy_gettext('End Date'), widget=AirflowDateTimePickerROWidget())
    state = SelectField(
        lazy_gettext('State'),
        choices=(
            ('success', 'success'),
            ('running', 'running'),
            ('failed', 'failed'),
            ('up_for_retry', 'up_for_retry'),
        ),
        widget=Select2Widget(),
        validators=[InputRequired()],
    )
    execution_date = DateTimeWithTimezoneField(
        lazy_gettext('Execution Date'),
        widget=AirflowDateTimePickerROWidget(),
        validators=[InputRequired()],
    )


class ConnectionForm(DynamicForm):
    """Form for editing and adding Connection"""

    conn_id = StringField(lazy_gettext('Conn Id'), validators=[InputRequired()], widget=BS3TextFieldWidget())
    description = StringField(lazy_gettext('Description'), widget=BS3TextAreaFieldWidget())
    host = StringField(lazy_gettext('Host'), widget=BS3TextFieldWidget())
    schema = StringField(lazy_gettext('Schema'), widget=BS3TextFieldWidget())
    login = StringField(lazy_gettext('Login'), widget=BS3TextFieldWidget())
    password = PasswordField(lazy_gettext('Password'), widget=BS3PasswordFieldWidget())
    port = IntegerField(lazy_gettext('Port'), validators=[Optional()], widget=BS3TextFieldWidget())
    extra = TextAreaField(lazy_gettext('Extra'), widget=BS3TextAreaFieldWidget())
