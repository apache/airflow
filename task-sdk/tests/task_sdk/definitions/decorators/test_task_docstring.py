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
from __future__ import annotations

import pendulum

from airflow.sdk import dag, task


def test_task_docstring_dedent_applied():
    """Test that task docstring is dedented when passed via function docstring."""

    @dag(schedule=None, start_date=pendulum.datetime(2022, 1, 1))
    def pipeline():
        @task
        def my_task():
            """
            This task does something important.

            In case of error you should do the following:
            1. Check the logs
            2. Verify the configuration
            3. Contact support
            """

        return my_task()

    dag_obj = pipeline()
    task_obj = dag_obj.task_dict["my_task"]

    # Verify that the docstring is dedented (no leading whitespace on each line)
    expected_doc = """This task does something important.

In case of error you should do the following:
1. Check the logs
2. Verify the configuration
3. Contact support"""
    assert task_obj.doc_md == expected_doc


def test_task_docstring_dedent_with_explicit_doc_md():
    """Test that explicit doc_md is not overridden by function docstring."""

    @dag(schedule=None, start_date=pendulum.datetime(2022, 1, 1))
    def pipeline():
        @task(doc_md="Explicit documentation")
        def my_task():
            """
            This is the function docstring.
            """

        return my_task()

    dag_obj = pipeline()
    task_obj = dag_obj.task_dict["my_task"]

    # Verify that explicit doc_md is used
    assert task_obj.doc_md == "Explicit documentation"


def test_task_docstring_dedent_with_multiline_indentation():
    """Test that task docstring with complex indentation is properly dedented."""

    @dag(schedule=None, start_date=pendulum.datetime(2022, 1, 1))
    def pipeline():
        @task
        def my_task():
            """
            Task description.

            This is a more complex example with nested indentation:

            - First level
                - Second level
                    - Third level

            And some code:

            ```python
            def example():
                return "value"
            ```
            """

        return my_task()

    dag_obj = pipeline()
    task_obj = dag_obj.task_dict["my_task"]

    # Verify that the docstring is dedented properly
    expected_doc = """Task description.

This is a more complex example with nested indentation:

- First level
    - Second level
        - Third level

And some code:

```python
def example():
    return "value"
```"""
    assert task_obj.doc_md == expected_doc


def test_task_no_docstring():
    """Test that task without docstring has no doc_md."""

    @dag(schedule=None, start_date=pendulum.datetime(2022, 1, 1))
    def pipeline():
        @task
        def my_task():
            pass

        return my_task()

    dag_obj = pipeline()
    task_obj = dag_obj.task_dict["my_task"]

    # Verify that doc_md is None when there's no docstring
    assert task_obj.doc_md is None


def test_task_docstring_dedent_simple():
    """Test that a simple indented docstring is dedented and stripped correctly."""

    @dag(schedule=None, start_date=pendulum.datetime(2022, 1, 1))
    def pipeline():
        @task
        def my_task():
            """My task description."""

        return my_task()

    dag_obj = pipeline()
    task_obj = dag_obj.task_dict["my_task"]

    # Verify leading/trailing whitespace is stripped
    assert task_obj.doc_md == "My task description."


def test_task_docstring_already_dedented():
    """Test that already-dedented docstrings are handled as a no-op by textwrap.dedent.

    When a docstring has no common leading whitespace, textwrap.dedent should
    return it unchanged and .strip() only removes surrounding whitespace.
    """
    import textwrap

    # Verify textwrap.dedent behavior on non-indented strings
    raw_doc = "This docstring has no leading indentation."
    assert textwrap.dedent(raw_doc).strip() == "This docstring has no leading indentation."

    # With a simple one-liner docstring (no common indent to strip)
    @dag(schedule=None, start_date=pendulum.datetime(2022, 1, 1))
    def pipeline():
        @task
        def my_task():
            """This docstring has no leading indentation."""

        return my_task()

    dag_obj = pipeline()
    task_obj = dag_obj.task_dict["my_task"]

    # The docstring should be unchanged after dedent + strip
    assert task_obj.doc_md == "This docstring has no leading indentation."
