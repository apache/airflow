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

from airflow.upgrade.rules.base_rule import BaseRule
from airflow.www_rbac.utils import CustomSQLAInterface


class UseCustomSQLAInterfaceClassRule(BaseRule):
    title = "Use CustomSQLAInterface instead of SQLAInterface for custom data models."

    description = """\
From Airflow 2.0, if you want to define your own Flask App Builder models you need to
use CustomSQLAInterface instead of SQLAInterface.

For Non-RBAC replace:

`from flask_appbuilder.models.sqla.interface import SQLAInterface`
`datamodel = SQLAInterface(your_data_model)`

with RBAC (in 1.10):

`from airflow.www_rbac.utils import CustomSQLAInterface`
`datamodel = CustomSQLAInterface(your_data_model)`

and in 2.0:

`from airflow.www.utils import CustomSQLAInterface`
`datamodel = CustomSQLAInterface(your_data_model)`
                  """

    def check(self):

        from airflow.plugins_manager import flask_appbuilder_views

        plugins_with_sqlainterface_data_model_instance = []

        if flask_appbuilder_views:
            for view_obj in flask_appbuilder_views:
                if not isinstance(view_obj.get("view").datamodel, CustomSQLAInterface):
                    plugins_with_sqlainterface_data_model_instance.append(view_obj.get("name"))

        if plugins_with_sqlainterface_data_model_instance:
            return (
                "Deprecation Warning: The following views: {} have "
                "data models instantiated "
                "from the SQLAInterface class.\n".format(plugins_with_sqlainterface_data_model_instance) +
                "See: "
                "https://github.com/apache/airflow/blob/master/"
                "UPDATING.md#use-customsqlainterface-instead-of-sqlainterface-for-custom-data-models"
            )
