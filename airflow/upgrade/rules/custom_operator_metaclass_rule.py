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

from __future__ import absolute_import

from airflow.configuration import conf
from airflow.models.dagbag import DagBag
from airflow.upgrade.rules.base_rule import BaseRule
from airflow.utils.db import provide_session


def check_task_for_metaclasses(task):
    class_type = type(task.__class__)
    if class_type != type:
        res = (
            "Class {class_name} contained invalid custom metaclass "
            "{metaclass_name}. Custom metaclasses for operators are not "
            "allowed in Airflow 2.0. Please remove this custom metaclass.".format(
                class_name=task.__class__, metaclass_name=class_type
            )
        )
        return res
    else:
        return None


class BaseOperatorMetaclassRule(BaseRule):
    title = "Ensure users are not using custom metaclasses in custom operators"

    description = """\
In Airflow 2.0, we require that all custom operators use the BaseOperatorMeta metaclass.\
To ensure this, we can no longer allow custom metaclasses in custom operators.
    """

    @provide_session
    def check(self, session=None):
        dagbag = DagBag(dag_folder=conf.get("core", "dags_folder"), include_examples=False)
        for dag_id, dag in dagbag.dags.items():
            for task in dag.tasks:
                res = check_task_for_metaclasses(task)
                if res:
                    yield res
