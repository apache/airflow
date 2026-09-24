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

import pytest
from check_contextmanager_class_decorators import check_file


class TestCheckFile:
    @pytest.mark.parametrize(
        "decorator",
        [
            pytest.param("@conf_vars({})", id="conf_vars"),
            pytest.param("@env_vars({})", id="env_vars"),
            pytest.param("@mock_plugin_manager(plugins=[])", id="mock_plugin_manager"),
            pytest.param("@contextmanager", id="contextmanager"),
            pytest.param("@contextlib.contextmanager", id="contextlib_contextmanager"),
        ],
    )
    def test_context_manager_on_a_test_class_is_reported(self, write_python_file, decorator):
        """Each of these turns the class into a function, so pytest silently collects nothing."""
        path = write_python_file(
            f"""
            {decorator}
            class TestSomething:
                def test_one(self):
                    pass
            """
        )
        errors = check_file(path)
        assert len(errors) == 1
        assert "TestSomething" in errors[0]

    @pytest.mark.parametrize(
        "code",
        [
            pytest.param(
                """
                @pytest.mark.usefixtures("no_plugins")
                class TestSomething:
                    def test_one(self):
                        pass
                """,
                id="usefixtures_is_the_supported_form",
            ),
            pytest.param(
                """
                class TestSomething:
                    @mock_plugin_manager(plugins=[])
                    def test_one(self):
                        pass
                """,
                id="on_a_method_is_fine",
            ),
            pytest.param(
                """
                @mock_plugin_manager(plugins=[])
                class HelperNotATestClass:
                    pass
                """,
                id="only_test_classes_are_checked",
            ),
        ],
    )
    def test_accepted_usages(self, write_python_file, code):
        assert check_file(write_python_file(code)) == []
