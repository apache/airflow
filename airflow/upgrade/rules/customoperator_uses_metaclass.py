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

from airflow.models import BaseOperator
from airflow.models.baseoperator import BaseOperatorMeta
from airflow.upgrade.rules.base_rule import BaseRule


class CustomOperatorUsesMetaclassRule(BaseRule):

    title = "BaseOperator uses metaclass"

    description = """\
BaseOperator class uses a BaseOperatorMeta as a metaclass. This metaclass is based on abc.ABCMeta.

If your custom operator uses different metaclass then you will have to adjust it."""

    def check(self):
        if not isinstance(BaseOperator, BaseOperatorMeta):
            return (
                "BaseOperator class uses `BaseOperatorMeta` as a metaclass by default. "
                "As your custom operator uses different metaclass, it would have to be adjusted."
            )
