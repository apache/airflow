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

Creating Custom TaskFlow Decorators
===================================

As of Airflow 2.2, users can now integrate custom decorators into their provider packages and have those decorators
appear natively as part of the ``@task.____`` design.

For an example. Let's say you were trying to create a "foo" decorator. To create ``@task.foo``, follow the following
steps:

1. Create a ``FooDecoratedOperator``

In this case, we are assuming that you have a ``FooOperator`` that takes a python function as an argument.
By creating a ``FooDecoratedOperator`` that inherits from ``FooOperator`` and
``airflow.decorators.base.DecoratedOperator``, Airflow will supply much of the needed functionality required to treat
your new class as a taskflow native class.

2. Create a ``foo_task`` function

Once you have your decorated class, create a function that takes arguments ``python_callable``\, ``multiple_outputs``\,
and ``kwargs``\. This function will use the ``airflow.decorators.base.task_decorator_factory`` function to convert
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

3. Register your new decorator in the provider.yaml of your provider

Finally, add a key-value of ``decorator-name``:``path-to-function`` to your provider.yaml. When Airflow starts, the
``ProviderManager`` class will automatically import this value and ``task.decorator-name`` will work as a new
decorator!

.. code-block:: yaml

   package-name: apache-airflow-providers-docker
   name: Docker
   description: |
       `Docker <https://docs.docker.com/install/>`__

   task-decorators:
       docker: airflow.providers.docker.operators.docker.docker_decorator


4. (Optional) Create a Mixin class so that your decorator will show up in your IDE's autocomplete

For better or worse, Python IDEs can not autocomplete dynamically
generated methods (see `here <https://intellij-support.jetbrains.com/hc/en-us/community/posts/115000665110-auto-completion-for-dynamic-module-attributes-in-python>`_).

To get around this, we had to find a solution that was "best possible." IDEs will only allow typing
through stub files, but we wanted to avoid any situation where a user would update their provider and the autocomplete
would be out of sync with the provider's actual parameters.

To hack around this problem, we found that you could extend the ``_TaskDecorator`` class in the ``__init__.pyi`` file
and the correct autocomplete will show up in the IDE.

To correctly implement this, please take the following steps:

Create a ``Mixin`` class for your decorator

Mixin classes are classes in python that tell the python interpreter that python can import them at any time.
Because they are not dependent on other classes, Mixin classes are great for multiple inheritance.

In the DockerDecorator we created a Mixin class that looks like this

.. exampleinclude:: ../howto/docker_decorator.py
    :language: python
    :dedent: 4
    :start-after: [START decoratormixin]
    :end-before: [END decoratormixin]

Notice that the function does not actually need to return anything. We will only use this class for type checking.

Once you have your Mixin class ready, go to ``airflow.decorators.__init__.pyi`` and add section similar to this

.. exampleinclude:: ../../../airflow/decorators/__init__.pyi
    :language: python
    :dedent: 4
    :start-after: [START import_docker]
    :end-before: [END import_docker]


This statement will prevent Airflow from failing if your provider does not exist.

Then at the bottom add a section to import your Mixin class

.. exampleinclude:: ../../../airflow/decorators/__init__.pyi
    :language: python
    :dedent: 4
    :start-after: [START extend_docker]
    :end-before: [END extend_docker]

Now once the next Airflow minor release comes out, users will be able to see your decorator in IDE autocomplete. This autocomplete will change based on the version of the provider that the user downloads.

Please note that this step is not required to create a working decorator but does create a better experience for developers.
