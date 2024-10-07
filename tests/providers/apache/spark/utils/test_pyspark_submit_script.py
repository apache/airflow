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

from io import StringIO

from airflow.providers.apache.spark.utils.pyspark_submit_script import write_pyspark_script


def _get_pyspark_script(**jinja_context) -> str:
    _buffer = StringIO()

    _default_jinja2_context = {
        "use_arguments": False,
        "use_spark_context": False,
        "use_spark_session": False,
        "input_filename": "INPUT_FILENAME.in",
        "pickling_library": "pickle",
        "python_callable": "test_func",
        "python_callable_source": "def test_func():\n    print('Hello, World!')",
        "expect_airflow": False,
    }
    _default_jinja2_context.update(jinja_context)

    write_pyspark_script(
        jinja_context=_default_jinja2_context,
        filename=_buffer,  # type: ignore
    )
    _buffer.seek(0)
    pyspark_source = _buffer.read()
    return pyspark_source


class TestWritePysparkScript:
    def test_write_pyspark_script_happy_path(self):
        pyspark_source = _get_pyspark_script()
        compile(pyspark_source, "pyspark_submit_script.py", "exec")

        lines = [line.rstrip() for line in pyspark_source.split("\n")]
        assert "import pickle" in lines
        assert "from pyspark import SparkFiles" in lines
        assert "from pyspark.sql import SparkSession" in lines
        assert "def test_func():" in lines
        assert "    print('Hello, World!')" in lines
        assert "_spark = SparkSession.builder.getOrCreate()" in lines
        assert "_sc = _spark.sparkContext" in lines
        assert 'arg_dict = {"args": [], "kwargs": {}}' in lines
        assert 'test_func(*arg_dict["args"], **arg_dict["kwargs"])' in lines

    def test_write_pyspark_script_with_use_arguments(self):
        pyspark_source = _get_pyspark_script(
            use_arguments=True,
            python_callable_source="def test_func(a: int, b: str):\n    print('Hello, World!')",
        )
        compile(pyspark_source, "pyspark_submit_script.py", "exec")

        lines = [line.rstrip() for line in pyspark_source.split("\n")]
        assert 'with open(SparkFiles.get("INPUT_FILENAME.in"), "rb") as file:' in lines
        assert "    arg_dict = pickle.load(file)" in lines
        assert "def test_func(a: int, b: str):" in lines

    def test_write_pyspark_script_with_use_spark_context(self):
        pyspark_source = _get_pyspark_script(
            use_spark_context=True,
            python_callable_source="def test_func(sc):\n    print('Hello, World!')",
        )
        compile(pyspark_source, "pyspark_submit_script.py", "exec")

        lines = [line.rstrip() for line in pyspark_source.split("\n")]
        assert "def test_func(sc):" in lines
        assert 'arg_dict = {"args": [], "kwargs": {}}' in lines
        assert 'arg_dict["kwargs"]["sc"] = _sc' in lines

    def test_write_pyspark_script_with_use_spark_session(self):
        pyspark_source = _get_pyspark_script(
            use_spark_session=True,
            python_callable_source="def test_func(spark):\n    print('Hello, World!')",
        )
        compile(pyspark_source, "pyspark_submit_script.py", "exec")

        lines = [line.rstrip() for line in pyspark_source.split("\n")]
        assert "def test_func(spark):" in lines
        assert 'arg_dict = {"args": [], "kwargs": {}}' in lines
        assert 'arg_dict["kwargs"]["spark"] = _spark' in lines

    def test_write_pyspark_script_with_use_dill(self):
        pyspark_source = _get_pyspark_script(
            use_arguments=True,
            pickling_library="dill",
            python_callable_source="def test_func(a: int, b: str, sc, spark):\n    print('Hello, World!')",
        )
        print(pyspark_source)
        compile(pyspark_source, "pyspark_submit_script.py", "exec")

        lines = [line.rstrip() for line in pyspark_source.split("\n")]
        assert "import dill" in lines
        assert "def test_func(a: int, b: str, sc, spark):" in lines
        assert 'with open(SparkFiles.get("INPUT_FILENAME.in"), "rb") as file:' in lines
        assert "    arg_dict = dill.load(file)" in lines

    def test_write_pyspark_script_with_expect_airflow(self):
        pyspark_source = _get_pyspark_script(expect_airflow=True)
        print(pyspark_source)
        compile(pyspark_source, "pyspark_submit_script.py", "exec")

        lines = [line.rstrip() for line in pyspark_source.split("\n")]
        assert "if sys.version_info >= (3,6):" in lines
        assert "        from airflow.plugins_manager import integrate_macros_plugins" in lines
        assert "        integrate_macros_plugins()" in lines
