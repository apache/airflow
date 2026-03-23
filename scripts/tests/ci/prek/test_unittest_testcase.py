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
from __future__ import annotations

from ci.prek.unittest_testcase import check_test_file


class TestCheckTestFile:
    def test_no_testcase_inheritance(self, write_python_file):
        path = write_python_file("""\
        class TestFoo:
            def test_something(self):
                pass
        """)
        assert check_test_file(str(path)) == 0

    def test_direct_testcase_inheritance(self, write_python_file):
        path = write_python_file("""\
        from unittest import TestCase

        class TestFoo(TestCase):
            def test_something(self):
                pass
        """)
        assert check_test_file(str(path)) == 1

    def test_attribute_testcase_inheritance(self, write_python_file):
        path = write_python_file("""\
        import unittest

        class TestFoo(unittest.TestCase):
            def test_something(self):
                pass
        """)
        assert check_test_file(str(path)) == 1

    def test_multiple_testcase_classes(self, write_python_file):
        path = write_python_file("""\
        from unittest import TestCase

        class TestFoo(TestCase):
            pass

        class TestBar(TestCase):
            pass
        """)
        assert check_test_file(str(path)) == 2

    def test_inherited_from_local_testcase_class(self, write_python_file):
        path = write_python_file("""\
        from unittest import TestCase

        class TestBase(TestCase):
            pass

        class TestChild(TestBase):
            pass
        """)
        # TestBase is detected first, then TestChild inherits from known class
        assert check_test_file(str(path)) == 2

    def test_no_classes(self, write_python_file):
        path = write_python_file("""\
        def test_something():
            pass
        """)
        assert check_test_file(str(path)) == 0

    def test_class_with_other_base(self, write_python_file):
        path = write_python_file("""\
        class TestFoo(SomeOtherBase):
            def test_something(self):
                pass
        """)
        assert check_test_file(str(path)) == 0

    def test_empty_file(self, write_python_file):
        path = write_python_file("")
        assert check_test_file(str(path)) == 0
