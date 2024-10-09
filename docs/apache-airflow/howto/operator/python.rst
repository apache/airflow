 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.



.. _howto/operator:PythonOperator:

PythonOperator
==============

Use the :class:`~airflow.operators.python.PythonOperator` to execute Python callables.

.. tip::
    The ``@task`` decorator is recommended over the classic ``PythonOperator`` to execute Python callables.

.. tab-set::

    .. tab-item:: @task
        :sync: taskflow

        .. exampleinclude:: /../../airflow/example_dags/example_python_decorator.py
            :language: python
            :dedent: 4
            :start-after: [START howto_operator_python]
            :end-before: [END howto_operator_python]

    .. tab-item:: PythonOperator
        :sync: operator

        .. exampleinclude:: /../../airflow/example_dags/example_python_operator.py
            :language: python
            :dedent: 4
            :start-after: [START howto_operator_python]
            :end-before: [END howto_operator_python]

Passing in arguments
^^^^^^^^^^^^^^^^^^^^

Pass extra arguments to the ``@task`` decorated function as you would with a normal Python function.

.. tab-set::

    .. tab-item:: @task
        :sync: taskflow

        .. exampleinclude:: /../../airflow/example_dags/example_python_decorator.py
            :language: python
            :dedent: 4
            :start-after: [START howto_operator_python_kwargs]
            :end-before: [END howto_operator_python_kwargs]

    .. tab-item:: PythonOperator
        :sync: operator

        .. exampleinclude:: /../../airflow/example_dags/example_python_operator.py
            :language: python
            :dedent: 4
            :start-after: [START howto_operator_python_kwargs]
            :end-before: [END howto_operator_python_kwargs]

Templating
^^^^^^^^^^

Airflow passes in an additional set of keyword arguments: one for each of the
:ref:`Jinja template variables <templates:variables>` and a ``templates_dict``
argument.

 ``templates_dict``, ``op_args``, ``op_kwargs`` arguments are templated, so each value in the dictionary
is evaluated as a :ref:`Jinja template <concepts:jinja-templating>`.

.. tab-set::

    .. tab-item:: @task
        :sync: taskflow

        .. exampleinclude:: /../../airflow/example_dags/example_python_decorator.py
            :language: python
            :dedent: 4
            :start-after: [START howto_operator_python_render_sql]
            :end-before: [END howto_operator_python_render_sql]

    .. tab-item:: PythonOperator
        :sync: operator

        .. exampleinclude:: /../../airflow/example_dags/example_python_operator.py
            :language: python
            :dedent: 4
            :start-after: [START howto_operator_python_render_sql]
            :end-before: [END howto_operator_python_render_sql]

Context
^^^^^^^

The ``Context`` is a dictionary object that contains information
about the environment of the ``DagRun``.
For example, selecting ``task_instance`` will get the currently running ``TaskInstance`` object.

It can be used implicitly, such as with ``**kwargs``,
but can also be used explicitly with ``get_current_context()``.
In this case, the type hint can be used for static analysis.

.. tab-set::

    .. tab-item:: @task
        :sync: taskflow

        .. exampleinclude:: /../../airflow/example_dags/example_python_context_decorator.py
            :language: python
            :dedent: 4
            :start-after: [START get_current_context]
            :end-before: [END get_current_context]

    .. tab-item:: PythonOperator
        :sync: operator

        .. exampleinclude:: /../../airflow/example_dags/example_python_context_operator.py
            :language: python
            :dedent: 4
            :start-after: [START get_current_context]
            :end-before: [END get_current_context]

.. _howto/operator:PythonVirtualenvOperator:

PythonVirtualenvOperator
========================

Use the :class:`~airflow.operators.python.PythonVirtualenvOperator` decorator to execute Python callables
inside a new Python virtual environment. The ``virtualenv`` package needs to be installed in the environment
that runs Airflow (as optional dependency ``pip install apache-airflow[virtualenv] --constraint ...``).

Additionally, the ``cloudpickle`` package needs to be installed as an optional dependency using command
``pip install [cloudpickle] --constraint ...``. This package is a replacement for currently used ``dill`` package.
Cloudpickle offers a strong advantage for its focus on standard pickling protocol, ensuring wider compatibility and
smoother data exchange, while still effectively handling common Python objects and global variables within functions.

.. tip::
    The ``@task.virtualenv`` decorator is recommended over the classic ``PythonVirtualenvOperator``
    to execute Python callables inside new Python virtual environments.

.. tab-set::

    .. tab-item:: @task.virtualenv
        :sync: taskflow

        .. exampleinclude:: /../../airflow/example_dags/example_python_decorator.py
            :language: python
            :dedent: 4
            :start-after: [START howto_operator_python_venv]
            :end-before: [END howto_operator_python_venv]

    .. tab-item:: PythonVirtualenvOperator
        :sync: operator

        .. exampleinclude:: /../../airflow/example_dags/example_python_operator.py
            :language: python
            :dedent: 4
            :start-after: [START howto_operator_python_venv]
            :end-before: [END howto_operator_python_venv]

Passing in arguments
^^^^^^^^^^^^^^^^^^^^

Pass extra arguments to the ``@task.virtualenv`` decorated function as you would with a normal Python function.
Unfortunately, Airflow does not support serializing ``var``, ``ti`` and ``task_instance`` due to incompatibilities
with the underlying library. For Airflow context variables make sure that you either have access to Airflow through
setting ``system_site_packages`` to ``True`` or add ``apache-airflow`` to the ``requirements`` argument.
Otherwise you won't have access to the most context variables of Airflow in ``op_kwargs``.
If you want the context related to datetime objects like ``data_interval_start`` you can add ``pendulum`` and
``lazy_object_proxy``.

Templating
^^^^^^^^^^

Jinja templating can be used in same way as described for the :ref:`howto/operator:PythonOperator`.

.. important::
    The Python function body defined to be executed is cut out of the DAG into a temporary file w/o surrounding code.
    As in the examples you need to add all imports again and you can not rely on variables from the global Python context.

    If you want to pass variables into the classic :class:`~airflow.operators.python.PythonVirtualenvOperator` use
    ``op_args`` and ``op_kwargs``.

If additional parameters for package installation are needed pass them in via the ``pip_install_options`` parameter or use a
``requirements.txt`` as in the example below:

.. code-block::

  SomePackage==0.2.1 --pre --index-url http://some.archives.com/archives
  AnotherPackage==1.4.3 --no-index --find-links /my/local/archives

All supported options are listed in the `requirements file format <https://pip.pypa.io/en/stable/reference/requirements-file-format/#supported-options>`_.

Virtual environment setup options
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The virtual environment is created based on the global python pip configuration on your worker. Using additional ENVs in your environment or adjustments in the general
pip configuration as described in `pip config <https://pip.pypa.io/en/stable/topics/configuration/>`_.

If you want to use additional task specific private python repositories to setup the virtual environment, you can pass the ``index_urls`` parameter which will adjust the
pip install configurations. Passed index urls replace the standard system configured index url settings.
To prevent adding secrets to the private repository in your DAG code you can use the Airflow
:doc:`../../authoring-and-scheduling/connections`. For this purpose the connection type ``Package Index (Python)`` can be used.

In the special case you want to prevent remote calls for setup of a virtual environment, pass the ``index_urls`` as empty list as ``index_urls=[]`` which
forced pip installer to use the ``--no-index`` option.

Caching and reuse
^^^^^^^^^^^^^^^^^

Setup of virtual environments is made per task execution in a temporary directory. After execution the virtual environment is deleted again. Ensure that the ``$tmp`` folder
on your workers have sufficient disk space. Usually (if not configured differently) the local pip cache will be used preventing a re-download of packages
for each execution.

But still setting up the virtual environment for every execution needs some time. For repeated execution you can set the option ``venv_cache_path`` to a file system
folder on your worker. In this case the virtual environment will be set up once and be re-used. If virtual environment caching is used, per unique requirements set different
virtual environment subfolders are created in the cache path. So depending on your variations in the DAGs in your system setup sufficient disk space is needed.

Note that no automated cleanup is made and in case of cached mode. All worker slots share the same virtual environment but if tasks are scheduled over and over on
different workers, it might happen that virtual environment are created on multiple workers individually. Also if the worker is started in a Kubernetes POD, a restart
of the worker will drop the cache (assuming ``venv_cache_path`` is not on a persistent volume).

In case you have problems during runtime with broken cached virtual environments, you can influence the cache directory hash by setting the Airflow variable
``PythonVirtualenvOperator.cache_key`` to any text. The content of this variable is uses in the vector to calculate the cache directory key.

Note that any modification of a cached virtual environment (like temp files in binary path, post-installing further requirements) might pollute a cached virtual environment and the
operator is not maintaining or cleaning the cache path.

Context
^^^^^^^

With some limitations, you can also use ``Context`` in virtual environments.

.. important::
    Using ``Context`` in a virtual environment is a bit of a challenge
    because it involves library dependencies and serialization issues.

    You can bypass this to some extent by using :ref:`Jinja template variables <templates:variables>` and explicitly passing it as a parameter.

    You can also use ``get_current_context()`` in the same way as before, but with some limitations.

    * Requires ``pydantic>=2``.

    * Set ``use_airflow_context`` to ``True`` to call ``get_current_context()`` in the virtual environment.

    * Set ``system_site_packages`` to ``True`` or set ``expect_airflow`` to ``True``

.. tab-set::

    .. tab-item:: @task.virtualenv
        :sync: taskflow

        .. exampleinclude:: /../../airflow/example_dags/example_python_context_decorator.py
            :language: python
            :dedent: 4
            :start-after: [START get_current_context_venv]
            :end-before: [END get_current_context_venv]

    .. tab-item:: PythonVirtualenvOperator
        :sync: operator

        .. exampleinclude:: /../../airflow/example_dags/example_python_context_operator.py
            :language: python
            :dedent: 4
            :start-after: [START get_current_context_venv]
            :end-before: [END get_current_context_venv]

.. _howto/operator:ExternalPythonOperator:

ExternalPythonOperator
======================

The ``ExternalPythonOperator`` can help you to run some of your tasks with a different set of Python
libraries than other tasks (and than the main Airflow environment). This might be a virtual environment
or any installation of Python that is preinstalled and available in the environment where Airflow
task is running. The operator takes Python binary as ``python`` parameter. Note, that even in case of
virtual environment, the ``python`` path should point to the python binary inside the virtual environment
(usually in ``bin`` subdirectory of the virtual environment). Contrary to regular use of virtual
environment, there is no need for ``activation`` of the environment. Merely using ``python`` binary
automatically activates it. In both examples below ``PATH_TO_PYTHON_BINARY`` is such a path, pointing
to the executable Python binary.

Use the :class:`~airflow.operators.python.ExternalPythonOperator` to execute Python callables inside a
pre-defined environment. The virtualenv package should be preinstalled in the environment where Python is run.
In case ``dill`` is used, it has to be preinstalled in the environment (the same version that is installed
in main Airflow environment).

.. tip::
    The ``@task.external_python`` decorator is recommended over the classic ``ExternalPythonOperator``
    to execute Python code in pre-defined Python environments.

.. tab-set::

    .. tab-item:: @task.external_python
        :sync: taskflow

        .. exampleinclude:: /../../airflow/example_dags/example_python_decorator.py
            :language: python
            :dedent: 4
            :start-after: [START howto_operator_external_python]
            :end-before: [END howto_operator_external_python]

    .. tab-item:: ExternalPythonOperator
        :sync: operator

        .. exampleinclude:: /../../airflow/example_dags/example_python_operator.py
            :language: python
            :dedent: 4
            :start-after: [START howto_operator_external_python]
            :end-before: [END howto_operator_external_python]


Passing in arguments
^^^^^^^^^^^^^^^^^^^^

Pass extra arguments to the ``@task.external_python`` decorated function as you would with a normal Python function.
Unfortunately Airflow does not support serializing ``var`` and ``ti`` / ``task_instance`` due to incompatibilities
with the underlying library. For Airflow context variables make sure that Airflow is also installed as part
of the virtualenv environment in the same version as the Airflow version the task is run on.
Otherwise you won't have access to the most context variables of Airflow in ``op_kwargs``.
If you want the context related to datetime objects like ``data_interval_start`` you can add ``pendulum`` and
``lazy_object_proxy`` to your virtual environment.

.. important::
    The Python function body defined to be executed is cut out of the DAG into a temporary file w/o surrounding code.
    As in the examples you need to add all imports again and you can not rely on variables from the global Python context.

    If you want to pass variables into the classic :class:`~airflow.operators.python.ExternalPythonOperator` use
    ``op_args`` and ``op_kwargs``.

Templating
^^^^^^^^^^

Jinja templating can be used in same way as described for the :ref:`howto/operator:PythonOperator`.

Context
^^^^^^^

You can use ``Context`` under the same conditions as ``PythonVirtualenvOperator``.

.. tab-set::

    .. tab-item:: @task.external_python
        :sync: taskflow

        .. exampleinclude:: /../../airflow/example_dags/example_python_context_decorator.py
            :language: python
            :dedent: 4
            :start-after: [START get_current_context_external]
            :end-before: [END get_current_context_external]

    .. tab-item:: ExternalPythonOperator
        :sync: operator

        .. exampleinclude:: /../../airflow/example_dags/example_python_context_operator.py
            :language: python
            :dedent: 4
            :start-after: [START get_current_context_external]
            :end-before: [END get_current_context_external]

.. _howto/operator:PythonBranchOperator:

PythonBranchOperator
====================

Use the :class:`~airflow.operators.python.PythonBranchOperator` to execute Python :ref:`branching <concepts:branching>`
tasks.

.. tip::
    The ``@task.branch`` decorator is recommended over the classic ``PythonBranchOperator``
    to execute Python code.

.. tab-set::

    .. tab-item:: @task.branch
        :sync: taskflow

        .. exampleinclude:: /../../airflow/example_dags/example_branch_operator_decorator.py
            :language: python
            :dedent: 4
            :start-after: [START howto_operator_branch_python]
            :end-before: [END howto_operator_branch_python]

    .. tab-item:: PythonBranchOperator
        :sync: operator

        .. exampleinclude:: /../../airflow/example_dags/example_branch_operator.py
            :language: python
            :dedent: 4
            :start-after: [START howto_operator_branch_python]
            :end-before: [END howto_operator_branch_python]

Passing in arguments and Templating
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Argument passing and templating options are the same like with :ref:`howto/operator:PythonOperator`.

.. _howto/operator:BranchPythonVirtualenvOperator:

BranchPythonVirtualenvOperator
==============================

Use the :class:`~airflow.operators.python.BranchPythonVirtualenvOperator` decorator to execute Python :ref:`branching <concepts:branching>`
tasks and is a hybrid of the :class:`~airflow.operators.python.PythonBranchOperator` with execution in a virtual environment.

.. tip::
    The ``@task.branch_virtualenv`` decorator is recommended over the classic
    ``BranchPythonVirtualenvOperator`` to execute Python code.

.. tab-set::

    .. tab-item:: @task.branch_virtualenv
        :sync: taskflow

        .. exampleinclude:: /../../airflow/example_dags/example_branch_operator_decorator.py
            :language: python
            :dedent: 4
            :start-after: [START howto_operator_branch_virtualenv]
            :end-before: [END howto_operator_branch_virtualenv]

    .. tab-item:: BranchPythonVirtualenvOperator
        :sync: operator

        .. exampleinclude:: /../../airflow/example_dags/example_branch_operator.py
            :language: python
            :dedent: 4
            :start-after: [START howto_operator_branch_virtualenv]
            :end-before: [END howto_operator_branch_virtualenv]

Passing in arguments and Templating
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Argument passing and templating options are the same like with :ref:`howto/operator:PythonOperator`.

.. _howto/operator:BranchExternalPythonOperator:

BranchExternalPythonOperator
============================

Use the :class:`~airflow.operators.python.BranchExternalPythonOperator` to execute Python :ref:`branching <concepts:branching>`
tasks and is a hybrid of the :class:`~airflow.operators.python.PythonBranchOperator` with execution in an
external Python environment.

.. tip::
    The ``@task.branch_external_python`` decorator is recommended over the classic
    ``BranchExternalPythonOperator`` to execute Python code.

.. tab-set::

    .. tab-item:: @task.branch_external_python
        :sync: taskflow

        .. exampleinclude:: /../../airflow/example_dags/example_branch_operator_decorator.py
            :language: python
            :dedent: 4
            :start-after: [START howto_operator_branch_ext_py]
            :end-before: [END howto_operator_branch_ext_py]

    .. tab-item:: BranchExternalPythonOperator
        :sync: operator

        .. exampleinclude:: /../../airflow/example_dags/example_branch_operator.py
            :language: python
            :dedent: 4
            :start-after: [START howto_operator_branch_ext_py]
            :end-before: [END howto_operator_branch_ext_py]


Passing in arguments and Templating
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Argument passing and templating options are the same like with :ref:`howto/operator:PythonOperator`.

.. _howto/operator:ShortCircuitOperator:

ShortCircuitOperator
====================

Use the :class:`~airflow.operators.python.ShortCircuitOperator` to control whether a pipeline continues
if a condition is satisfied or a truthy value is obtained.

The evaluation of this condition and truthy value is done via the output of a callable. If the
callable returns True or a truthy value, the pipeline is allowed to continue and an :ref:`XCom <concepts:xcom>`
of the output will be pushed. If the output is False or a falsy value, the pipeline will be short-circuited
based on the configured short-circuiting (more on this later). In the example below, the tasks that follow the
"condition_is_true" task will execute while the tasks downstream of the "condition_is_false" task will be
skipped.

.. tip::
    The ``@task.short_circuit`` decorator is recommended over the classic ``ShortCircuitOperator``
    to short-circuit pipelines via Python callables.

.. tab-set::

    .. tab-item:: @task.short_circuit
        :sync: taskflow

        .. exampleinclude:: /../../airflow/example_dags/example_short_circuit_decorator.py
            :language: python
            :dedent: 4
            :start-after: [START howto_operator_short_circuit]
            :end-before: [END howto_operator_short_circuit]

    .. tab-item:: ShortCircuitOperator
        :sync: operator

        .. exampleinclude:: /../../airflow/example_dags/example_short_circuit_operator.py
            :language: python
            :dedent: 4
            :start-after: [START howto_operator_short_circuit]
            :end-before: [END howto_operator_short_circuit]


The "short-circuiting" can be configured to either respect or ignore the :ref:`trigger rule <concepts:trigger-rules>`
defined for downstream tasks. If ``ignore_downstream_trigger_rules`` is set to True, the default configuration, all
downstream tasks are skipped without considering the ``trigger_rule`` defined for tasks.  If this parameter is
set to False, the direct downstream tasks are skipped but the specified ``trigger_rule`` for other subsequent
downstream tasks are respected. In this short-circuiting configuration, the operator assumes the direct
downstream task(s) were purposely meant to be skipped but perhaps not other subsequent tasks. This
configuration is especially useful if only *part* of a pipeline should be short-circuited rather than all
tasks which follow the short-circuiting task.

In the example below, notice that the "short_circuit" task is configured to respect downstream trigger
rules. This means while the tasks that follow the "short_circuit" task will be skipped
since the decorated function returns False, "task_7" will still execute as its set to execute when upstream
tasks have completed running regardless of status (i.e. the ``TriggerRule.ALL_DONE`` trigger rule).

.. tab-set::

    .. tab-item:: @task.short_circuit
        :sync: taskflow

        .. exampleinclude:: /../../airflow/example_dags/example_short_circuit_decorator.py
            :language: python
            :dedent: 4
            :start-after: [START howto_operator_short_circuit_trigger_rules]
            :end-before: [END howto_operator_short_circuit_trigger_rules]

    .. tab-item:: ShortCircuitOperator
        :sync: operator

        .. exampleinclude:: /../../airflow/example_dags/example_short_circuit_operator.py
            :language: python
            :dedent: 4
            :start-after: [START howto_operator_short_circuit_trigger_rules]
            :end-before: [END howto_operator_short_circuit_trigger_rules]

Passing in arguments
^^^^^^^^^^^^^^^^^^^^

Pass extra arguments to the ``@task.short_circuit``-decorated function as you would with a normal Python function.


Templating
^^^^^^^^^^

Jinja templating can be used in same way as described for the :ref:`howto/operator:PythonOperator`.

.. _howto/operator:PythonSensor:

PythonSensor
============

The :class:`~airflow.sensors.python.PythonSensor` executes an arbitrary callable and waits for its return
value to be True.

.. tip::
    The ``@task.sensor`` decorator is recommended over the classic ``PythonSensor``
    to execute Python callables to check for True condition.

.. tab-set::

    .. tab-item:: @task.sensor
        :sync: taskflow

        .. exampleinclude:: /../../airflow/example_dags/example_sensor_decorator.py
            :language: python
            :dedent: 4
            :start-after: [START wait_function]
            :end-before: [END wait_function]

    .. tab-item:: PythonSensor
        :sync: operator

        .. exampleinclude:: /../../airflow/example_dags/example_sensors.py
            :language: python
            :dedent: 4
            :start-after: [START example_python_sensors]
            :end-before: [END example_python_sensors]

Templating
^^^^^^^^^^

Airflow passes in an additional set of keyword arguments: one for each of the
:ref:`Jinja template variables <templates:variables>` and a ``templates_dict``
argument.

 ``templates_dict``, ``op_args``, ``op_kwargs`` arguments are templated, so each value in the dictionary
is evaluated as a :ref:`Jinja template <concepts:jinja-templating>`.
