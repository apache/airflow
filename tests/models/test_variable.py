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

import unittest

from cryptography.fernet import Fernet

from airflow import settings
from airflow.models import Variable, crypto
from tests.test_utils.config import conf_vars


class TestVariable(unittest.TestCase):
    def setUp(self):
        crypto._fernet = None

    def tearDown(self):
        crypto._fernet = None

    @conf_vars({('core', 'fernet_key'): ''})
    def test_variable_no_encryption(self):
        """
        Test variables without encryption
        """
        Variable.set('key', 'value')
        session = settings.Session()
        test_var = session.query(Variable).filter(Variable.key == 'key').one()
        self.assertFalse(test_var.is_encrypted)
        self.assertEqual(test_var.val, 'value')

    @conf_vars({('core', 'fernet_key'): Fernet.generate_key().decode()})
    def test_variable_with_encryption(self):
        """
        Test variables with encryption
        """
        Variable.set('key', 'value')
        session = settings.Session()
        test_var = session.query(Variable).filter(Variable.key == 'key').one()
        self.assertTrue(test_var.is_encrypted)
        self.assertEqual(test_var.val, 'value')

    def test_var_with_encryption_rotate_fernet_key(self):
        """
        Tests rotating encrypted variables.
        """
        key1 = Fernet.generate_key()
        key2 = Fernet.generate_key()

        with conf_vars({('core', 'fernet_key'): key1.decode()}):
            Variable.set('key', 'value')
            session = settings.Session()
            test_var = session.query(Variable).filter(Variable.key == 'key').one()
            self.assertTrue(test_var.is_encrypted)
            self.assertEqual(test_var.val, 'value')
            self.assertEqual(Fernet(key1).decrypt(test_var._val.encode()), b'value')

        # Test decrypt of old value with new key
        with conf_vars({('core', 'fernet_key'): ','.join([key2.decode(), key1.decode()])}):
            crypto._fernet = None
            self.assertEqual(test_var.val, 'value')

            # Test decrypt of new value with new key
            test_var.rotate_fernet_key()
            self.assertTrue(test_var.is_encrypted)
            self.assertEqual(test_var.val, 'value')
            self.assertEqual(Fernet(key2).decrypt(test_var._val.encode()), b'value')

    def test_variable_set_get_round_trip(self):
        Variable.set("tested_var_set_id", "Monday morning breakfast")
        self.assertEqual("Monday morning breakfast", Variable.get("tested_var_set_id"))

    def test_variable_set_get_round_trip_json(self):
        value = {"a": 17, "b": 47}
        Variable.set("tested_var_set_id", value, serialize_json=True)
        self.assertEqual(value, Variable.get("tested_var_set_id", deserialize_json=True))

    def test_get_non_existing_var_should_return_default(self):
        default_value = "some default val"
        self.assertEqual(default_value, Variable.get("thisIdDoesNotExist",
                                                     default_var=default_value))

    def test_get_non_existing_var_should_raise_key_error(self):
        with self.assertRaises(KeyError):
            Variable.get("thisIdDoesNotExist")

    def test_get_non_existing_var_with_none_default_should_return_none(self):
        self.assertIsNone(Variable.get("thisIdDoesNotExist", default_var=None))

    def test_get_non_existing_var_should_not_deserialize_json_default(self):
        default_value = "}{ this is a non JSON default }{"
        self.assertEqual(default_value, Variable.get("thisIdDoesNotExist",
                                                     default_var=default_value,
                                                     deserialize_json=True))

    def test_variable_setdefault_round_trip(self):
        key = "tested_var_setdefault_1_id"
        value = "Monday morning breakfast in Paris"
        Variable.setdefault(key, value)
        self.assertEqual(value, Variable.get(key))

    def test_variable_setdefault_round_trip_json(self):
        key = "tested_var_setdefault_2_id"
        value = {"city": 'Paris', "Happiness": True}
        Variable.setdefault(key, value, deserialize_json=True)
        self.assertEqual(value, Variable.get(key, deserialize_json=True))

    def test_variable_setdefault_existing_json(self):
        key = "tested_var_setdefault_2_id"
        value = {"city": 'Paris', "Happiness": True}
        Variable.set(key, value, serialize_json=True)
        val = Variable.setdefault(key, value, deserialize_json=True)
        # Check the returned value, and the stored value are handled correctly.
        self.assertEqual(value, val)
        self.assertEqual(value, Variable.get(key, deserialize_json=True))

    def test_variable_delete(self):
        key = "tested_var_delete"
        value = "to be deleted"

        # No-op if the variable doesn't exist
        Variable.delete(key)
        with self.assertRaises(KeyError):
            Variable.get(key)

        # Set the variable
        Variable.set(key, value)
        self.assertEqual(value, Variable.get(key))

        # Delete the variable
        Variable.delete(key)
        with self.assertRaises(KeyError):
            Variable.get(key)
