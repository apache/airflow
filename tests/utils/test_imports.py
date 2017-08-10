# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import types
import unittest

from airflow.utils.imports import import_class_by_name

class TestImports(unittest.TestCase):

    def test_import_class_by_name(self):
        cls_name = 'airflow.operators.bash_operator.BashOperator'
        invalid_cls_name_1 = 'airflow.operators.bash_operator.BashOpera'
        invalid_cls_name_2 = 'airflow.operators.bash_operator'

        cls_reference = import_class_by_name(cls_name)
        self.assertEqual(cls_reference.__name__, "BashOperator")
        self.assertTrue(callable(cls_reference))

        self.assertRaises(AttributeError, import_class_by_name, invalid_cls_name_1)

        invalid_cls_reference = import_class_by_name(invalid_cls_name_2)
        self.assertEqual(type(invalid_cls_reference), types.ModuleType)
        self.assertFalse(callable(invalid_cls_reference))
