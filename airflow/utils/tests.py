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

import re
import unittest

from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
from airflow.utils.decorators import apply_defaults


def skipUnlessImported(module, obj):
    import importlib
    try:
        m = importlib.import_module(module)
    except ImportError:
        m = None
    return unittest.skipUnless(
        obj in dir(m),
        "Skipping test because {} could not be imported from {}".format(
            obj, module))


def assertEqualIgnoreMultipleSpaces(case, first, second, msg=None):
    def _trim(s):
        return re.sub(r"\s+", " ", s.strip())
    return case.assertEqual(_trim(first), _trim(second), msg)


# Custom Operator and extra Operator Links used for Tests in tests_views.py
class AirflowLink(BaseOperatorLink):
    """
    Operator Link for Apache Airflow Website
    """
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


class CustomBaseOperator(BaseOperator):
    operator_extra_links = ()

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(CustomBaseOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.log.info("Hello World!")


class GoogleLink(BaseOperatorLink):
    """
    Operator Link for Apache Airflow Website for Google
    """
    name = 'google'
    operators = [Dummy3TestOperator, CustomBaseOperator]

    def get_link(self, operator, dttm):
        return 'https://www.google.com'


class AirflowLink2(BaseOperatorLink):
    """
    Operator Link for Apache Airflow Website for 1.10.5
    """
    name = 'airflow'
    operators = [Dummy2TestOperator, Dummy3TestOperator]

    def get_link(self, operator, dttm):
        return 'https://airflow.apache.org/1.10.5/'


class GithubLink(BaseOperatorLink):
    """
    Operator Link for Apache Airflow Github
    """
    name = 'github'

    def get_link(self, operator, dttm):
        return 'https://github.com/apache/airflow'
