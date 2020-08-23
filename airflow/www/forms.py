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
from operator import itemgetter

import pendulum
from flask_appbuilder.fieldwidgets import (
    BS3PasswordFieldWidget, BS3TextAreaFieldWidget, BS3TextFieldWidget, Select2Widget,
)
from flask_appbuilder.forms import DynamicForm
from flask_babel import lazy_gettext
from flask_wtf import FlaskForm
from wtforms import widgets
from wtforms.fields import (
    BooleanField, Field, IntegerField, PasswordField, SelectField, StringField, TextAreaField,
)
from wtforms.validators import DataRequired, NumberRange, Optional

from airflow.configuration import conf
from airflow.utils import timezone
from airflow.utils.types import DagRunType
from airflow.www.validators import ValidJson
from airflow.www.widgets import AirflowDateTimePickerWidget


class DateTimeWithTimezoneField(Field):
    """
    A text field which stores a `datetime.datetime` matching a format.
    """
    widget = widgets.TextInput()

    def __init__(self, label=None, validators=None, datetime_format='%Y-%m-%d %H:%M:%S%Z', **kwargs):
        super(DateTimeWithTimezoneField, self).__init__(label, validators, **kwargs)
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
    """
    Date filter form needed for task views
    """
    execution_date = DateTimeWithTimezoneField(
        "Execution date", widget=AirflowDateTimePickerWidget())


class DateTimeWithNumRunsForm(FlaskForm):
    """
    Date time and number of runs form for tree view, task duration
    and landing times
    """

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
    """
    Date time and number of runs and dag runs form for graph and gantt view
    """
    execution_date = SelectField("DAG run")


class DagRunForm(DynamicForm):
    """Form for editing and adding DAG Run"""
    dag_id = StringField(
        lazy_gettext('Dag Id'),
        validators=[DataRequired()],
        widget=BS3TextFieldWidget())
    start_date = DateTimeWithTimezoneField(
        lazy_gettext('Start Date'),
        widget=AirflowDateTimePickerWidget())
    end_date = DateTimeWithTimezoneField(
        lazy_gettext('End Date'),
        widget=AirflowDateTimePickerWidget())
    run_id = StringField(
        lazy_gettext('Run Id'),
        validators=[DataRequired()],
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
        validators=[ValidJson(), Optional()],
        widget=BS3TextAreaFieldWidget())

    def populate_obj(self, item):
        """Populates the attributes of the passed obj with data from the form’s fields."""
        super().populate_obj(item)  # pylint: disable=no-member
        item.run_type = DagRunType.from_run_id(item.run_id).value
        if item.conf:
            item.conf = json.loads(item.conf)


_connection_types = [
    ('docker', 'Docker Registry'),
    ('elasticsearch', 'Elasticsearch'),
    ('exasol', 'Exasol'),
    ('facebook_social', 'Facebook Social'),
    ('fs', 'File (path)'),
    ('ftp', 'FTP'),
    ('google_cloud_platform', 'Google Cloud Platform'),
    ('hdfs', 'HDFS'),
    ('http', 'HTTP'),
    ('pig_cli', 'Pig Client Wrapper'),
    ('hive_cli', 'Hive Client Wrapper'),
    ('hive_metastore', 'Hive Metastore Thrift'),
    ('hiveserver2', 'Hive Server 2 Thrift'),
    ('jdbc', 'JDBC Connection'),
    ('odbc', 'ODBC Connection'),
    ('jenkins', 'Jenkins'),
    ('mysql', 'MySQL'),
    ('postgres', 'Postgres'),
    ('oracle', 'Oracle'),
    ('vertica', 'Vertica'),
    ('presto', 'Presto'),
    ('s3', 'S3'),
    ('samba', 'Samba'),
    ('sqlite', 'Sqlite'),
    ('ssh', 'SSH'),
    ('cloudant', 'IBM Cloudant'),
    ('mssql', 'Microsoft SQL Server'),
    ('mesos_framework-id', 'Mesos Framework ID'),
    ('jira', 'JIRA'),
    ('redis', 'Redis'),
    ('wasb', 'Azure Blob Storage'),
    ('databricks', 'Databricks'),
    ('aws', 'Amazon Web Services'),
    ('emr', 'Elastic MapReduce'),
    ('snowflake', 'Snowflake'),
    ('segment', 'Segment'),
    ('sqoop', 'Sqoop'),
    ('azure_batch', 'Azure Batch Service'),
    ('azure_data_lake', 'Azure Data Lake'),
    ('azure_container_instances', 'Azure Container Instances'),
    ('azure_cosmos', 'Azure CosmosDB'),
    ('azure_data_explorer', 'Azure Data Explorer'),
    ('cassandra', 'Cassandra'),
    ('qubole', 'Qubole'),
    ('mongo', 'MongoDB'),
    ('gcpcloudsql', 'Google Cloud SQL'),
    ('grpc', 'GRPC Connection'),
    ('yandexcloud', 'Yandex Cloud'),
    ('livy', 'Apache Livy'),
    ('tableau', 'Tableau'),
    ('kubernetes', 'Kubernetes cluster Connection'),
    ('spark', 'Spark'),
    ('imap', 'IMAP'),
    ('vault', 'Hashicorp Vault'),
    ('azure', 'Azure'),
]


class ConnectionForm(DynamicForm):
    """Form for editing and adding Connection"""

    conn_id = StringField(
        lazy_gettext('Conn Id'),
        widget=BS3TextFieldWidget())
    conn_type = SelectField(
        lazy_gettext('Conn Type'),
        choices=sorted(_connection_types, key=itemgetter(1)),  # pylint: disable=protected-access
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
        validators=[Optional()],
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
        validators=[NumberRange(min=0)],
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
    extra__yandexcloud__service_account_json = PasswordField(
        lazy_gettext('Service account auth JSON'),
        widget=BS3PasswordFieldWidget(),
        description='Service account auth JSON. Looks like '
                    '{"id", "...", "service_account_id": "...", "private_key": "..."}. '
                    'Will be used instead of OAuth token and SA JSON file path field if specified.',
    )
    extra__yandexcloud__service_account_json_path = StringField(
        lazy_gettext('Service account auth JSON file path'),
        widget=BS3TextFieldWidget(),
        description='Service account auth JSON file path. File content looks like '
                    '{"id", "...", "service_account_id": "...", "private_key": "..."}. '
                    'Will be used instead of OAuth token if specified.',
    )
    extra__yandexcloud__oauth = PasswordField(
        lazy_gettext('OAuth Token'),
        widget=BS3PasswordFieldWidget(),
        description='User account OAuth token. Either this or service account JSON must be specified.',
    )
    extra__yandexcloud__folder_id = StringField(
        lazy_gettext('Default folder ID'),
        widget=BS3TextFieldWidget(),
        description='Optional. This folder will be used to create all new clusters and nodes by default',
    )
    extra__yandexcloud__public_ssh_key = StringField(
        lazy_gettext('Public SSH key'),
        widget=BS3TextFieldWidget(),
        description='Optional. This key will be placed to all created Compute nodes'
        'to let you have a root shell there',
    )
    extra__kubernetes__in_cluster = BooleanField(
        lazy_gettext('In cluster configuration'))
    extra__kubernetes__kube_config = StringField(
        lazy_gettext('Kube config (JSON format)'),
        widget=BS3TextFieldWidget())
    extra__kubernetes__namespace = StringField(
        lazy_gettext('Namespace'),
        widget=BS3TextFieldWidget())
