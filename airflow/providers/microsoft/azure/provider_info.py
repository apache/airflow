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

from flask_appbuilder.fieldwidgets import BS3TextFieldWidget, BS3PasswordFieldWidget
from flask_babel import lazy_gettext
from wtforms import StringField, PasswordField, IntegerField
from wtforms.validators import NumberRange

try:
    from airflow.www.forms import ConnectionForm
except ImportError:
    from airflow.www_rbac.forms import ConnectionForm  # type:ignore

PROVIDER_NAME = "microsoft.azure"
PROVIDER_VERSION = "2020.09.10"
PROVIDER_URL = "https://azure.microsoft.com/"
PROVIDER_DOCS = "https://airflow.readthedocs.io/en/latest/operators-and-hooks-ref.html#azure-microsoft-azure"

CONN_TYPE_TO_HOOK = {
    "azure_batch": (
        "airflow.providers.microsoft.azure.hooks.azure_batch.AzureBatchHook",
        "azure_batch_conn_id",
    ),
    "azure_cosmos": (
        "airflow.providers.microsoft.azure.hooks.azure_cosmos.AzureCosmosDBHook",
        "azure_cosmos_conn_id",
    ),
    "azure_data_lake": (
        "airflow.providers.microsoft.azure.hooks.azure_data_lake.AzureDataLakeHook",
        "azure_data_lake_conn_id",
    ),
    "wasb": ("airflow.providers.microsoft.azure.hooks.wasb.WasbHook", "wasb_conn_id"),
}

connection_types = [
    ('azure_batch', 'Azure Batch Service'),
    ('azure_data_lake', 'Azure Data Lake'),
    ('azure_container_instances', 'Azure Container Instances'),
    ('azure_cosmos', 'Azure CosmosDB'),
    ('azure_data_explorer', 'Azure Data Explorer'),
    ('wasb', 'Azure Blob Storage'),
    ('azure', 'Azure'),
]

ConnectionForm.extra__google_cloud_platform__project = StringField(
    lazy_gettext('Project Id'), widget=BS3TextFieldWidget()
)
ConnectionForm.extra__google_cloud_platform__key_path = StringField(
    lazy_gettext('Keyfile Path'), widget=BS3TextFieldWidget()
)
ConnectionForm.extra__google_cloud_platform__keyfile_dict = PasswordField(
    lazy_gettext('Keyfile JSON'), widget=BS3PasswordFieldWidget()
)
ConnectionForm.extra__google_cloud_platform__scope = StringField(
    lazy_gettext('Scopes (comma separated)'), widget=BS3TextFieldWidget()
)
ConnectionForm.extra__google_cloud_platform__num_retries = IntegerField(
    lazy_gettext('Number of Retries'), validators=[NumberRange(min=0)], widget=BS3TextFieldWidget(), default=5
)
