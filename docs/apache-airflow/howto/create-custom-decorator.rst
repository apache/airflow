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

2. Create a ``foo_task`` function

    Once you have your decorated class, create a function that takes arguments ``python_callable``, ``multiple_outputs``,
    and ``kwargs``. This function will use the ``airflow.decorators.base.task_decorator_factory`` function to convert
    the new ``FooDecoratedOperator`` into a TaskFlow function decorator!

    .. code-block:: python

       def foo_task(
           python_callable: Optional[Callable] = None,
           multiple_outputs: Optional[bool] = None,
           **kwargs
       ):
           return task_decorator_factory(
               python_callable=python_callable,
               multiple_outputs=multiple_outputs,
               decorated_operator_class=FooDecoratedOperator,
               **kwargs,
           )

3. Register your new decorator in get_provider_info of your provider

    Finally, add a key-value ``task-decortor`` to the dict returned from the provider entrypoint. This should be
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
                        "class-name": ["name.of.python.package.foo_task"],
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

To get around this, we had to find a solution that was "best possible." IDEs will only allow typing
through stub files, but we wanted to avoid any situation where a user would update their provider and the auto-complete
would be out of sync with the provider's actual parameters.

To hack around this problem, we found that you could extend the ``_TaskDecorator`` class in the ``airflow/decorators/__init__.py`` inside an ``if TYPE_CHECKING`` block
and the correct auto-complete will show up in the IDE.

The first step is to create a ``Mixin`` class for your decorator.

Mixin classes are classes in python that tell the python interpreter that python can import them at any time.
Because they are not dependent on other classes, Mixin classes are great for multiple inheritance.

In the DockerDecorator we created a Mixin class that looks like this:

.. exampleinclude:: ../../../airflow/providers/docker/decorators/docker.py
    :language: python
    :start-after: [START decoratormixin]
    :end-before: [END decoratormixin]

Notice that the function does not actually need to return anything as we only use this class for type checking. Sadly you will have to duplicate the args, defaults and types from your real FooOperator in order for them to show up in auto-completion prompts.

Once you have your Mixin class ready, go to ``airflow/decorators/__init__.py`` and add section similar to this

.. exampleinclude:: ../../../airflow/decorators/__init__.py
    :language: python
    :start-after: [START mixin_for_autocomplete]
    :end-before: [END mixin_for_autocomplete]

The ``if TYPE_CHECKING`` guard means that this code will only be used for type checking (such as mypy) or generating IDE auto-completion. Catching the ``ImportError`` is important as

Once the change is merged and the next Airflow (minor or patch) release comes out, users will be able to see your decorator in IDE auto-complete. This auto-complete will change based on the version of the provider that the user has installed.

Please note that this step is not required to create a working decorator but does create a better experience for users of the provider.
