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

from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
from airflow.utils.decorators import apply_defaults


class AirflowLink(BaseOperatorLink):
    name = 'airflow'

    def get_link(self, operator, dttm):
        return 'should_be_overridden'


class Dummy2TestOperator(BaseOperator):
    """
    Example of an Operator that has an extra operator link
    and will be overriden by the one defined in tests/plugins/test_plugin.py
    """
    operator_extra_links = (
        AirflowLink(),
    )


class Dummy3TestOperator(BaseOperator):
    """
    Example of an operator that has no extra Operator link.
    An operator link would be added to this operator via Airflow plugin
    """
    operator_extra_links = ()


# Custom Operator and extra Link
class GoogleLink(BaseOperatorLink):
    name = 'google'

    def get_link(self, operator, dttm):
        return 'https://www.google.com'


class CustomBaseOperator(BaseOperator):
    operator_extra_links = (GoogleLink(),)

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(CustomBaseOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.log.info("Hello World!")
