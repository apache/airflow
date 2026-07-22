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

Creating Custom ``@task`` Decorators
====================================

As of Airflow 2.2 it is possible to add custom decorators to the TaskFlow interface from within a provider
package and have those decorators appear natively as part of the ``@task.____`` design.

This is useful when you already have an operator that should execute a Python callable, but you want DAG
authors to use it with the TaskFlow API:

.. code-block:: python

    from airflow.sdk import task


    @task.foo(task_id="run_in_foo")
    def transform_customer_data(customer_id: str):
        return {"customer_id": customer_id}

At a high level, a custom TaskFlow decorator has three parts:

* a decorated operator class that combines your operator with
  ``airflow.sdk.bases.decorator.DecoratedOperator``;
* a small factory function that calls ``airflow.sdk.bases.decorator.task_decorator_factory``;
* provider metadata that tells Airflow the decorator name and the import path of the factory function.

The examples below use ``FooOperator`` and register it as ``@task.foo``. ``FooOperator`` is assumed to be
an operator you already maintain in your provider.

If you only need a helper inside one DAG file or one internal project, you may not need a registered
``@task.foo`` decorator at all. A regular Python function that wraps ``@task`` can be enough. Registering a
new ``@task.<name>`` decorator is the provider-level approach: it makes the decorator discoverable anywhere
the provider is installed.

1. Create a decorated operator
------------------------------

Create a decorated operator that inherits from both ``DecoratedOperator`` and the operator you want to
expose through TaskFlow:

.. code-block:: python

    from __future__ import annotations

    from collections.abc import Callable, Collection, Mapping
    from typing import Any

    from airflow.sdk.bases.decorator import DecoratedOperator
    from airflow.providers.foo.operators.foo import FooOperator


    class FooDecoratedOperator(DecoratedOperator, FooOperator):
        """Wrap a Python callable in FooOperator for use with ``@task.foo``."""

        custom_operator_name = "@task.foo"

        def __init__(
            self,
            *,
            python_callable: Callable,
            op_args: Collection[Any] | None = None,
            op_kwargs: Mapping[str, Any] | None = None,
            **kwargs,
        ) -> None:
            super().__init__(
                python_callable=python_callable,
                op_args=op_args,
                op_kwargs=op_kwargs,
                **kwargs,
            )

``DecoratedOperator`` captures the decorated Python function and the arguments passed when the task is
called in the DAG. Your operator stays responsible for the actual execution behavior.

Set ``custom_operator_name`` to the public decorator name. This is the name users see in UI and task
representations. For example, the Docker provider uses ``@task.docker``.

If the base operator already has arguments named ``python_callable``, ``op_args``, or ``op_kwargs``, pass
those values through ``kwargs_to_upstream`` when calling ``DecoratedOperator.__init__``. The standard
``@task`` implementation does this because ``PythonOperator`` also uses those argument names.

2. Create the decorator factory function
----------------------------------------

Create a function that turns ``FooDecoratedOperator`` into a TaskFlow decorator:

.. code-block:: python

    from __future__ import annotations

    from collections.abc import Callable
    from typing import TYPE_CHECKING

    from airflow.sdk.bases.decorator import task_decorator_factory

    if TYPE_CHECKING:
        from airflow.sdk.bases.decorator import TaskDecorator


    def foo_task(
        python_callable: Callable | None = None,
        multiple_outputs: bool | None = None,
        **kwargs,
    ) -> "TaskDecorator":
        return task_decorator_factory(
            python_callable=python_callable,
            multiple_outputs=multiple_outputs,
            decorated_operator_class=FooDecoratedOperator,
            **kwargs,
        )

Users can then write either ``@task.foo`` or ``@task.foo(...)``. Any keyword arguments that are not handled
by ``task_decorator_factory`` are forwarded to ``FooDecoratedOperator`` when the decorated function is
called.

.. note::

    Airflow providers that support both Airflow 2 and Airflow 3 often import ``DecoratedOperator``,
    ``TaskDecorator``, and ``task_decorator_factory`` from ``airflow.providers.common.compat.sdk`` instead
    of importing from ``airflow.sdk`` directly. Use the compatibility module when your provider needs that
    wider version support.

3. Register the decorator in provider metadata
----------------------------------------------

Airflow discovers custom TaskFlow decorators from provider metadata. The metadata entry contains:

``name``
    The attribute that will be added under ``task``. For ``@task.foo``, use ``foo``.

``class-name``
    The import path to the decorator factory function, not the decorated operator class.

For providers in the Airflow repository, add the entry to the provider's ``provider.yaml`` file:

.. code-block:: yaml

    task-decorators:
      - name: foo
        class-name: airflow.providers.foo.decorators.foo.foo_task

The generated ``get_provider_info.py`` file will include this metadata after provider metadata is
regenerated. Do not edit the generated file directly in the Airflow repository.

For third-party providers that expose provider metadata from a Python entrypoint, add the same information
to the dict returned by ``get_provider_info`` as described in
:doc:`apache-airflow-providers:howto/create-custom-providers`:

.. code-block:: python

    def get_provider_info():
        return {
            "package-name": "foo-provider-airflow",
            "name": "Foo",
            "task-decorators": [
                {
                    "name": "foo",
                    "class-name": "airflow.providers.foo.decorators.foo.foo_task",
                }
            ],
            # ...
        }

The ``name`` must be a valid Python identifier. When Airflow starts, ``ProviderManager`` reads the
``task-decorators`` metadata and makes the factory function available as ``task.foo``.

(Optional) Adding IDE auto-completion support
---------------------------------------------

.. note::

    This section mostly applies to the apache-airflow managed providers. We have not decided if we will allow third-party providers to register auto-completion in this way.

For better or worse, Python IDEs can not auto-complete dynamically
generated methods (see `JetBrain's write up on the subject <https://intellij-support.jetbrains.com/hc/en-us/community/posts/115000665110-auto-completion-for-dynamic-module-attributes-in-python>`_).

To hack around this problem, a type stub ``airflow/sdk/definitions/decorators/__init__.pyi`` is provided to statically declare
the type signature of each task decorator. A newly added task decorator should declare its signature stub
like this:

.. exampleinclude:: ../../../task-sdk/src/airflow/sdk/definitions/decorators/__init__.pyi
    :language: python
    :start-after: [START decorator_signature]
    :end-before: [END decorator_signature]

The signature should allow only keyword-only arguments, including one named ``multiple_outputs`` that's
automatically provided by default. All other arguments should be copied directly from the real FooOperator,
and we recommend adding a comment to explain what arguments are filled automatically by FooDecoratedOperator
and thus not included.

If the new decorator can be used without arguments (e.g. ``@task.python`` instead of ``@task.python()``),
You should also add an overload that takes a single callable immediately after the "real" definition so mypy
can recognize the function as a "bare decorator":

.. exampleinclude:: ../../../task-sdk/src/airflow/sdk/definitions/decorators/__init__.pyi
    :language: python
    :start-after: [START mixin_for_typing]
    :end-before: [END mixin_for_typing]

Once the change is merged and the next Airflow (minor or patch) release comes out, users will be able to see your decorator in IDE auto-complete. This auto-complete will change based on the version of the provider that the user has installed.

Please note that this step is not required to create a working decorator, but does create a better experience for users of the provider.
