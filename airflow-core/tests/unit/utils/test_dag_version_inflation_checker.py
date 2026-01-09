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

import ast

from airflow.utils.dag_version_inflation_checker import (
    AirflowRuntimeVaryingValueChecker,
    DagTaskDetector,
    RuntimeVaryingValueAnalyzer,
    RuntimeVaryingValueWarning,
    WarningContext,
)


class TestRuntimeVaryingValueAnalyzer:
    def setup_method(self):
        """Each test gets a fresh analyzer instance."""
        self.varying_vars = {}
        self.imports = {}
        self.from_imports = {}
        self.analyzer = RuntimeVaryingValueAnalyzer(self.varying_vars, self.imports, self.from_imports)

    def test_is_runtime_varying_attribute_call__detects_datetime_now(self):
        """datetime.now() should be recognized as runtime-varying."""
        code = "datetime.now()"
        call_node = ast.parse(code, mode="eval").body
        self.imports["datetime"] = "datetime"

        # The func is an Attribute node: datetime.now
        assert isinstance(call_node.func, ast.Attribute)
        result = self.analyzer.is_runtime_varying_attribute_call(call_node.func)

        assert result is True

    def test_is_runtime_varying_attribute_call__ignores_static_method(self):
        """Static methods like str.upper() should NOT be detected."""
        code = "str.upper('hello')"
        call_node = ast.parse(code, mode="eval").body

        assert isinstance(call_node.func, ast.Attribute)
        result = self.analyzer.is_runtime_varying_attribute_call(call_node.func)

        assert result is False

    def test_is_runtime_varying_attribute_call__handles_aliased_imports(self):
        """
        Should detect runtime-varying calls even with import aliases.

        Example: import datetime as dt; dt.now()
        """
        code = "dt.now()"
        call_node = ast.parse(code, mode="eval").body
        self.imports["dt"] = "datetime"  # dt is alias for datetime

        assert isinstance(call_node.func, ast.Attribute)
        result = self.analyzer.is_runtime_varying_attribute_call(call_node.func)

        assert result is True

    def test_is_runtime_varying_name_call__detects_uuid4(self):
        """Detect uuid4() when imported as "from uuid import uuid4."""
        code = "uuid4()"
        call_node = ast.parse(code, mode="eval").body
        self.from_imports["uuid4"] = ("uuid", "uuid4")

        assert isinstance(call_node.func, ast.Name)
        result = self.analyzer.is_runtime_varying_name_call(call_node.func)

        assert result is True

    def test_is_runtime_varying_name_call__ignores_regular_function(self):
        code = "my_function()"
        call_node = ast.parse(code, mode="eval").body

        assert isinstance(call_node.func, ast.Name)
        result = self.analyzer.is_runtime_varying_name_call(call_node.func)

        assert result is False

    def test_has_varying_arguments__detects_varying_positional_arg(self):
        """
        Detect when a positional argument is runtime-varying.

        Example: print(datetime.now())
        """
        code = "print(datetime.now())"
        call_node = ast.parse(code, mode="eval").body
        self.imports["datetime"] = "datetime"

        result = self.analyzer.has_varying_arguments(call_node)

        assert result is True

    def test_has_varying_arguments__detects_varying_keyword_arg(self):
        """
        Detect when a keyword argument is runtime-varying.

        Example: func(param=random.randint(1, 10))
        """
        code = "func(param=func1(random.randint(1, 10)))"
        call_node = ast.parse(code, mode="eval").body
        self.imports["random"] = "random"

        result = self.analyzer.has_varying_arguments(call_node)

        assert result is True

    def test_has_varying_arguments__returns_false_for_static_args(self):
        """
        Static arguments should return False.

        Example: print("hello", 123)
        """
        code = 'print("hello", 123)'
        call_node = ast.parse(code, mode="eval").body

        result = self.analyzer.has_varying_arguments(call_node)

        assert result is False

    def test_is_runtime_varying_call__true_when_function_itself_varies(self):
        """
        Return True when the function call itself is runtime-varying.

        Example: datetime.now() - the function is the varying part
        """
        code = "datetime.now()"
        call_node = ast.parse(code, mode="eval").body
        self.imports["datetime"] = "datetime"

        result = self.analyzer.is_runtime_varying_call(call_node)

        assert result is True

    def test_is_runtime_varying_call__true_when_argument_varies(self):
        """
        Return True when arguments contain runtime-varying values.

        Example: print(datetime.now()) - print is static but arg varies
        """
        code = "print(datetime.now())"
        call_node = ast.parse(code, mode="eval").body
        self.imports["datetime"] = "datetime"

        result = self.analyzer.is_runtime_varying_call(call_node)

        assert result is True

    def test_is_runtime_varying_call__false_when_completely_static(self):
        """Return False when both function and arguments are static."""
        code = 'print("hello")'
        call_node = ast.parse(code, mode="eval").body

        result = self.analyzer.is_runtime_varying_call(call_node)

        assert result is False

    def test_get_varying_source__detects_direct_call(self):
        """Detect direct runtime-varying function calls."""
        code = "datetime.now()"
        node = ast.parse(code, mode="eval").body
        self.imports["datetime"] = "datetime"

        result = self.analyzer.get_varying_source(node)

        assert result == "datetime.now()"

    def test_get_varying_source__detects_variable_reference(self):
        """
        Detect when a variable holds a runtime-varying value.

        Example: current_time = datetime.now();
        """
        code = "current_time"
        node = ast.parse(code, mode="eval").body
        self.varying_vars["current_time"] = (10, "datetime.now()")

        result = self.analyzer.get_varying_source(node)

        assert result == "datetime.now()"

    def test_get_varying_source__detects_in_fstring(self):
        """
        Detect runtime-varying values embedded in f-strings.

        Example: f"dag_{datetime.now()}"
        """
        code = 'f"dag_{datetime.now()}"'
        node = ast.parse(code, mode="eval").body
        self.imports["datetime"] = "datetime"

        result = self.analyzer.get_varying_source(node)

        assert result == "datetime.now()"

    def test_get_varying_source__detects_in_list(self):
        """
        Detect runtime-varying values inside list literals.

        Example: [1, 2, datetime.now()]
        """
        code = "[1, 2, datetime.now()]"
        node = ast.parse(code, mode="eval").body
        self.imports["datetime"] = "datetime"

        result = self.analyzer.get_varying_source(node)

        assert result == "datetime.now()"

    def test_get_varying_source__detects_in_dict_value(self):
        """
        Detect runtime-varying values in dictionary values.

        Example: {"key": datetime.now()}
        """
        code = '{"key": datetime.now()}'
        node = ast.parse(code, mode="eval").body
        self.imports["datetime"] = "datetime"

        result = self.analyzer.get_varying_source(node)

        assert result == "datetime.now()"

    def test_get_varying_source__detects_in_binary_operation(self):
        """
        Detect runtime-varying values in binary operations.

        Example: "prefix_" + str(datetime.now())
        """
        code = '"prefix_" + str(datetime.now())'
        node = ast.parse(code, mode="eval").body
        self.imports["datetime"] = "datetime"

        result = self.analyzer.get_varying_source(node)

        assert result is not None
        assert "datetime.now()" in result

    def test_get_varying_source__returns_none_for_static_values(self):
        """
        Return None for completely static values.

        Example: "static_string", 123, [1, 2, 3]
        """
        static_values = [
            '"static_string"',
            "123",
            "[1, 2, 3]",
            '{"key": "value"}',
        ]

        for code in static_values:
            node = ast.parse(code, mode="eval").body
            result = self.analyzer.get_varying_source(node)
            assert result is None, f"Expected None for static value: {code}"


class TestDAGTaskDetector:
    def setup_method(self):
        """Each test gets a fresh detector instance"""
        self.from_imports = {}
        self.detector = DagTaskDetector(self.from_imports)

    def test_is_dag_constructor__detects_traditional_dag_call_uppercase(self):
        """
        Detect uppercase DAG() when imported.

        Usage: dag = DAG(dag_id="my_dag")
        """
        code = 'DAG(dag_id="my_dag")'
        call_node = ast.parse(code, mode="eval").body
        self.from_imports["DAG"] = ("airflow", "DAG")

        result = self.detector.is_dag_constructor(call_node)

        assert result is True

    def test_is_dag_constructor__detects_dag_generated_by_decorator(self):
        """
        Detect Dag generated by decorator.

        Usage: @dag(dag_id="my_dag")
        """
        code = 'dag(dag_id="my_dag")'
        call_node = ast.parse(code, mode="eval").body
        self.from_imports["dag"] = ("airflow.decorators", "dag")

        result = self.detector.is_dag_constructor(call_node)

        assert result is True

    def test_is_dag_constructor__ignores_non_dag_functions(self):
        """Regular function calls should not be detected as Dag constructors."""
        code = "my_function()"
        call_node = ast.parse(code, mode="eval").body

        result = self.detector.is_dag_constructor(call_node)

        assert result is False

    def test_is_task_constructor__true_when_inside_dag_context(self):
        """
        Any function call inside a Dag with-block is considered a task.

        Example:
            with DAG() as dag:
                PythonOperator()  # <- This is a task
        """
        code = "PythonOperator(task_id='my_task')"
        call_node = ast.parse(code, mode="eval").body

        self.detector.enter_dag_context()
        result = self.detector.is_task_constructor(call_node)

        assert result is True

    def test_is_task_constructor__false_when_outside_dag_context(self):
        """Same call outside Dag context is NOT automatically a task."""
        code = "PythonOperator(task_id='my_task')"
        call_node = ast.parse(code, mode="eval").body

        result = self.detector.is_task_constructor(call_node)
        assert result is False

    def test_is_task_constructor__true_when_dag_passed_as_argument(self):
        """
        Detect task when dag= parameter references a Dag instance.

        Example: my_dag = DAG(dag_id='dag); task = PythonOperator(dag=my_dag)
        """
        code = "PythonOperator(task_id='task', dag=my_dag)"
        call_node = ast.parse(code, mode="eval").body
        self.detector.register_dag_instance("my_dag")

        result = self.detector.is_task_constructor(call_node)
        assert result is True

    def test_is_task_constructor__true_when_dag_in_positional_args(self):
        """
        Detect task even when Dag is passed as positional argument.

        Example: my_dag = DAG(dag_id='dag); task = PythonOperator('task_id', my_dag)
        """
        code = "PythonOperator('task_id', my_dag)"
        call_node = ast.parse(code, mode="eval").body
        self.detector.register_dag_instance("my_dag")

        result = self.detector.is_task_constructor(call_node)
        assert result is True

    def test_enter_and_exit_dag_context(self):
        """Properly track entering and exiting Dag with-blocks."""
        assert self.detector.is_in_dag_context is False

        self.detector.enter_dag_context()
        assert self.detector.is_in_dag_context is True

        self.detector.exit_dag_context()
        assert self.detector.is_in_dag_context is False

    def test_register_dag_instance(self):
        """Remember variable names that hold Dag instances."""
        assert "my_dag" not in self.detector.dag_instances

        self.detector.register_dag_instance("my_dag")

        assert "my_dag" in self.detector.dag_instances


class TestAirflowRuntimeVaryingValueChecker:
    """Tests for AirflowRuntimeVaryingValueChecker (Main Visitor)."""

    def setup_method(self):
        """Each test gets a fresh checker instance"""
        self.checker = AirflowRuntimeVaryingValueChecker()

    def test_visit_import__tracks_simple_import(self):
        """Remember simple imports like 'import datetime'."""
        code = "import datetime"
        tree = ast.parse(code)

        self.checker.visit(tree)

        assert "datetime" in self.checker.imports
        assert self.checker.imports["datetime"] == "datetime"

    def test_visit_import__tracks_aliased_import(self):
        """Remember import aliases like 'import datetime as dt'."""
        code = "import datetime as dt"
        tree = ast.parse(code)

        self.checker.visit(tree)

        assert "dt" in self.checker.imports
        assert self.checker.imports["dt"] == "datetime"

    def test_visit_importfrom__tracks_from_import(self):
        """Remember 'from X import Y' style imports."""
        code = "from datetime import now"
        tree = ast.parse(code)

        self.checker.visit(tree)

        assert "now" in self.checker.from_imports
        assert self.checker.from_imports["now"] == ("datetime", "now")

    def test_visit_importfrom__tracks_aliased_from_import(self):
        """Remember aliases in from imports."""
        code = "from datetime import now as current_time"
        tree = ast.parse(code)

        self.checker.visit(tree)

        assert "current_time" in self.checker.from_imports
        assert self.checker.from_imports["current_time"] == ("datetime", "now")

    def test_visit_assign__registers_dag_instance(self):
        """When assigning DAG(), remember the variable name."""
        code = """
from airflow import DAG
my_dag = DAG(dag_id="test")
"""
        tree = ast.parse(code)

        self.checker.visit(tree)

        assert "my_dag" in self.checker.dag_detector.dag_instances

    def test_visit_assign__tracks_varying_variable(self):
        """When assigning a runtime-varying value, track the variable."""
        code = """
from datetime import datetime
current_time = datetime.now()
"""
        tree = ast.parse(code)

        self.checker.visit(tree)

        assert "current_time" in self.checker.varying_vars
        line, source = self.checker.varying_vars["current_time"]
        assert "datetime.now()" in source

    def test_visit_assign__warns_on_dag_with_varying_value(self):
        """Warn when Dag constructor uses runtime-varying values."""
        code = """
from airflow import DAG
from datetime import datetime
dag = DAG(dag_id=f"dag_{datetime.now()}")
"""
        tree = ast.parse(code)

        self.checker.visit(tree)

        assert len(self.checker.static_check_result.warnings) == 1
        assert any("Dag constructor" in w.message for w in self.checker.static_check_result.warnings)

    def test_visit_call__detects_task_in_dag_context(self):
        """Detect task creation inside Dag with block."""
        code = """
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(dag_id="test") as dag:
    task = PythonOperator(task_id=f"task_{datetime.now()}") # !problem
"""
        tree = ast.parse(code)

        self.checker.visit(tree)

        assert len(self.checker.static_check_result.warnings) == 1
        assert any("PythonOperator" in w.code for w in self.checker.static_check_result.warnings)

    def test_visit_for__warns_on_varying_range(self):
        """Warn when for-loop range is runtime-varying."""
        code = """
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id=dag_id,
    schedule_interval='@daily',
) as dag:
    for i in [datetime.now(), "3"]:
        task = BashOperator(
            task_id='print_bash_hello_{i}',
            bash_command=f'echo "Hello from DAG {i}!"',  # !problem
            dag=dag,
        )
"""
        tree = ast.parse(code)

        self.checker.visit(tree)
        warnings = self.checker.static_check_result.warnings

        assert len(warnings) == 1
        assert any("BashOperator" in w.code for w in warnings)

    def test_check_and_warn__creates_warning_for_varying_arg(self):
        """Create a warning when detecting varying positional argument."""
        code = 'DAG(f"dag_{datetime.now()}")'
        call_node = ast.parse(code, mode="eval").body
        self.checker.from_imports["DAG"] = ("airflow", "DAG")
        self.checker.imports["datetime"] = "datetime"

        self.checker._check_and_warn(call_node, WarningContext.DAG_CONSTRUCTOR)

        assert len(self.checker.static_check_result.warnings) == 1
        warning = self.checker.static_check_result.warnings[0]
        assert WarningContext.DAG_CONSTRUCTOR.value in warning.message
        assert "datetime.now()" in warning.code

    def test_check_and_warn__creates_warning_for_varying_kwarg(self):
        """Create a warning when detecting varying keyword argument"""
        code = "DAG(dag_id=datetime.now())"
        call_node = ast.parse(code, mode="eval").body
        self.checker.from_imports["DAG"] = ("airflow", "DAG")
        self.checker.imports["datetime"] = "datetime"

        self.checker._check_and_warn(call_node, WarningContext.TASK_CONSTRUCTOR)

        assert len(self.checker.static_check_result.warnings) == 1
        warning = self.checker.static_check_result.warnings[0]
        assert "dag_id" in warning.code
        assert "datetime.now()" in warning.code


class TestIntegrationScenarios:
    """
    Integration tests showing real-world Airflow patterns.
    Demonstrate actual use cases and why they're problematic.
    """

    def _check_code(self, code: str) -> list[RuntimeVaryingValueWarning]:
        """Helper to parse and check code"""
        tree = ast.parse(code)
        checker = AirflowRuntimeVaryingValueChecker()
        checker.visit(tree)
        return checker.static_check_result.warnings

    def test_antipattern__dynamic_dag_id_with_timestamp(self):
        """ANTI-PATTERN: Using timestamps in Dag IDs."""
        code = """
from airflow import DAG
from datetime import datetime

# BAD: Dag ID includes current timestamp
dag = DAG(dag_id=f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
"""
        warnings = self._check_code(code)

        assert len(warnings) == 1
        assert any("datetime.now()" in w.code for w in warnings)

    def test_define_dag_with_block(self):
        code = """
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import uuid
from datetime import datetime as dt

start_date = dt.now()

default_args = {
    'start_date': start_date
}

with DAG(
    dag_id="my_dag",
    default_args=default_args # !problem
) as dag, Test(default_args=default_args) as test:
    task1 = PythonOperator(
        task_id=f"task_{uuid.uuid4()}", # !problem
        python_callable=lambda: None
    )

    task2 = BashOperator(
        task_id=f"task_{dt.now()}" # !problem
    )

    task3 = BashOperator(
        task_id="task_for_normal_case"
    )

    task1 >> task2 >> task3
"""
        warnings = self._check_code(code)

        assert len(warnings) == 3
        assert any("uuid.uuid4()" in w.code for w in warnings)
        assert any("dt.now()" in w.code for w in warnings)
        assert any("default_args" in w.code for w in warnings)

    def test_correct_pattern__static_dag_with_runtime_context(self):
        code = """
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from datetime import datetime
from mydule import test_function

import time

current_timestamp = time.time()
local_time = time.localtime()

dag = DAG(
    dag_id='time_module_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    params={
        "execution_date": Param(
            default=f"manual_run_{datetime.now().isoformat()}", # !problem
            description="Unique identifier for the run",
            type="string",
            minLength=10,
        )
    },
)

b = test_function(time=current_timestamp)

task1 = BashOperator(
    task_id='time_task',
    bash_command=f'echo "Timestamp: {current_timestamp}"',  # !problem
    dag=dag,
)

task2 = BashOperator(
    task_id='time_task2',
    dag=dag,
)

task1 >> task2
"""
        warnings = self._check_code(code)

        assert len(warnings) == 2
        assert any("Param(default=f'manual_run_{datetime.now().isoformat()}'" in w.code for w in warnings)
        assert any("current_timestamp" in w.code for w in warnings)

    def test_dag_decorator_pattern__currently_not_detected(self):
        """
        PATTERN: @dag decorator usage
        """
        code = """
from airflow.decorators import dag, task
from datetime import datetime

@dag(dag_id=f"my_dag_{datetime.now()}") # !problem
def my_dag_function():

    @task
    def my_task():
        return "hello"

    my_task()
"""
        warnings = self._check_code(code)
        assert len(warnings) == 1

    def test_dag_generated_in_for_or_function_statement(self):
        code = """
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import pendulum

def create_dag(dag_id, task_id):
    default_args = {
        "depends_on_past": False,
        "start_date": datetime.now()
    }

    with DAG(
        dag_id,
        default_args=default_args, # !problem
    ) as dag:
        task1 = BashOperator(
            task_id=task_id
        )

    return dag

now = pendulum.now()
seoul = now.in_timezone('Asia/Seoul')

for i in [datetime.now(), "3"]:
    dag_id = f"dag_{i}_{random.randint(1, 1000)}"

    dag = DAG(
        dag_id=dag_id,  # !problem
        schedule_interval='@daily',
        tags=[f"iteration_{i}"],
    )

    task1 = BashOperator(
        task_id='print_bash_hello',
        bash_command=f'echo "Hello from DAG {i}!"',  # !problem
        dag=dag,
    )

    task2 = BashOperator(
        task_id=f'random_task_{random.randint(1, 100)}',  # !problem
        bash_command='echo "World"',
        dag=dag,
    )

    task3 = BashOperator(
        task_id=f'random_task_in_{seoul}',  # !problem
        bash_command='echo "World"',
        dag=dag,
    )

    task1 >> task2 >> task3
"""
        warnings = self._check_code(code)
        assert len(warnings) == 5
