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

As of Airflow 2.2 it is possible add custom decorators to the TaskFlow interface from within a provider
package and have those decorators appear natively as part of the ``@task.____`` design.

For an example. Let's say you were trying to create an easier mechanism to run python functions as "foo"
tasks. The steps to create and register ``@task.foo`` are:

1. Create a ``FooDecoratedOperator``

    In this case, we are assuming that you have an existing ``FooOperator`` that takes a python function as an
    argument.  By creating a ``FooDecoratedOperator`` that inherits from ``FooOperator`` and
    ``airflow.decorators.base.DecoratedOperator``, Airflow will supply much of the needed functionality required
    to treat your new class as a taskflow native class.

    You should also override the ``custom_operator_name`` attribute to provide a custom name for the task. For
    example, ``_DockerDecoratedOperator`` in the ``apache-airflow-providers-docker`` provider sets this to
    ``@task.docker`` to indicate the decorator name it implements.

2. Create a ``foo_task`` function

    Once you have your decorated class, create a function like this, to convert
    the new ``FooDecoratedOperator`` into a TaskFlow function decorator!

    .. code-block:: python

        from typing import TYPE_CHECKING
        from airflow.decorators.base import task_decorator_factory

        if TYPE_CHECKING:
            from airflow.decorators.base import TaskDecorator


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

3. Register your new decorator in get_provider_info of your provider

    Finally, add a key-value ``task-decorators`` to the dict returned from the provider entrypoint. This should be
    a list with each item containing ``name`` and ``class-name`` keys. When Airflow starts, the
    ``ProviderManager`` class will automatically import this value and ``task.foo`` will work as a new decorator!

    .. code-block:: python

        def get_provider_info():
            return {
                "package-name": "foo-provider-airflow",
                "name": "Foo",
                "task-decorators": [
                    {
                        "name": "foo",
                        # "Import path" and function name of the `foo_task`
                        "class-name": "name.of.python.package.foo_task",
                    }
                ],
                # ...
            }

    Please note that the ``name`` must be a valid python identifier.

(Optional) Adding IDE auto-completion support
=============================================

.. note::

    This section mostly applies to the apache-airflow managed providers. We have not decided if we will allow third-party providers to register auto-completion in this way.

For better or worse, Python IDEs can not auto-complete dynamically
generated methods (see `JetBrain's write up on the subject <https://intellij-support.jetbrains.com/hc/en-us/community/posts/115000665110-auto-completion-for-dynamic-module-attributes-in-python>`_).

To hack around this problem, a type stub ``airflow/decorators/__init__.pyi`` is provided to statically declare
the type signature of each task decorator. A newly added task decorator should declare its signature stub
like this:

.. exampleinclude:: ../../../airflow/decorators/__init__.pyi
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

.. exampleinclude:: ../../../airflow/decorators/__init__.pyi
    :language: python
    :start-after: [START mixin_for_typing]
    :end-before: [END mixin_for_typing]

Once the change is merged and the next Airflow (minor or patch) release comes out, users will be able to see your decorator in IDE auto-complete. This auto-complete will change based on the version of the provider that the user has installed.

Please note that this step is not required to create a working decorator, but does create a better experience for users of the provider.
