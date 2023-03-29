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

.. _custom_operator:

Creating a custom Operator
==========================


Airflow allows you to create new operators to suit the requirements of you or your team.
This extensibility is one of the many features which make Apache Airflow powerful.

You can create any operator you want by extending the :class:`airflow.models.baseoperator.BaseOperator`

There are two methods that you need to override in a derived class:

* Constructor - Define the parameters required for the operator. You only need to specify the arguments specific to your operator.
  You can specify the ``default_args`` in the DAG file. See :ref:`Default args <concepts-default-arguments>` for more details.

* Execute - The code to execute when the runner calls the operator. The method contains the
  Airflow context as a parameter that can be used to read config values.

.. note::

    When implementing custom operators, do not make any expensive operations in the ``__init__`` method. The operators
    will be instantiated once per scheduler cycle per task using them, and making database calls can significantly slow
    down scheduling and waste resources.

Let's implement an example ``HelloOperator`` in a new file ``hello_operator.py``:

.. code-block:: python

        from airflow.models.baseoperator import BaseOperator


        class HelloOperator(BaseOperator):
            def __init__(self, name: str, **kwargs) -> None:
                super().__init__(**kwargs)
                self.name = name

            def execute(self, context):
                message = f"Hello {self.name}"
                print(message)
                return message

.. note::

    For imports to work, you should place the file in a directory that
    is present in the :envvar:`PYTHONPATH` env. Airflow adds ``dags/``, ``plugins/``, and ``config/`` directories
    in the Airflow home to :envvar:`PYTHONPATH` by default. e.g., In our example,
    the file is placed in the ``custom_operator/`` directory.
    See :doc:`/administration-and-deployment/modules_management` for details on how Python and Airflow manage modules.

You can now use the derived custom operator as follows:

.. code-block:: python

    from custom_operator.hello_operator import HelloOperator

    with dag:
        hello_task = HelloOperator(task_id="sample-task", name="foo_bar")

You also can keep using your plugins folder for storing your custom operators. If you have the file
``hello_operator.py`` within the plugins folder, you can import the operator as follows:

.. code-block:: python

    from hello_operator import HelloOperator

If an operator communicates with an external service (API, database, etc) it's a good idea
to implement the communication layer using a :ref:`custom-operator/hook`. In this way the implemented logic
can be reused by other users in different operators. Such approach provides better decoupling and
utilization of added integration than using ``CustomServiceBaseOperator`` for each external service.

Other consideration is the temporary state. If an operation requires an in-memory state (for example
a job id that should be used in ``on_kill`` method to cancel a request) then the state should be keep
in the operator not in a hook. In this way the service hook can be completely state-less and whole
logic of an operation is in one place - in the operator.

.. _custom-operator/hook:

Hooks
-----
Hooks act as an interface to communicate with the external shared resources in a DAG.
For example, multiple tasks in a DAG can require access to a MySQL database. Instead of
creating a connection per task, you can retrieve a connection from the hook and utilize it.
Hook also helps to avoid storing connection auth parameters in a DAG.
See :doc:`connection` for how to create and manage connections and :doc:`apache-airflow-providers:index` for
details of how to add your custom connection types via providers.

Let's extend our previous example to fetch name from MySQL:

.. code-block:: python

    class HelloDBOperator(BaseOperator):
        def __init__(self, name: str, mysql_conn_id: str, database: str, **kwargs) -> None:
            super().__init__(**kwargs)
            self.name = name
            self.mysql_conn_id = mysql_conn_id
            self.database = database

        def execute(self, context):
            hook = MySqlHook(mysql_conn_id=self.mysql_conn_id, schema=self.database)
            sql = "select name from user"
            result = hook.get_first(sql)
            message = f"Hello {result['name']}"
            print(message)
            return message

When the operator invokes the query on the hook object, a new connection gets created if it doesn't exist.
The hook retrieves the auth parameters such as username and password from Airflow
backend and passes the params to the :py:func:`airflow.hooks.base.BaseHook.get_connection`.
You should create hook only in the ``execute`` method or any method which is called from ``execute``.
The constructor gets called whenever Airflow parses a DAG which happens frequently. And instantiating a hook
there will result in many unnecessary database connections.
The ``execute`` gets called only during a DAG run.


User interface
--------------
Airflow also allows the developer to control how the operator shows up in the DAG UI.
Override ``ui_color`` to change the background color of the operator in UI.
Override ``ui_fgcolor`` to change the color of the label.
Override ``custom_operator_name`` to change the displayed name to something other than the classname.

.. code-block:: python

        class HelloOperator(BaseOperator):
            ui_color = "#ff0000"
            ui_fgcolor = "#000000"
            custom_operator_name = "Howdy"
            # ...

Templating
----------
You can use :ref:`Jinja templates <concepts:jinja-templating>` to parameterize your operator.
Airflow considers the field names present in ``template_fields``  for templating while rendering
the operator.

.. code-block:: python

        class HelloOperator(BaseOperator):

            template_fields: Sequence[str] = ("name",)

            def __init__(self, name: str, world: str, **kwargs) -> None:
                super().__init__(**kwargs)
                self.name = name
                self.world = world

            def execute(self, context):
                message = f"Hello {self.world} it's {self.name}!"
                print(message)
                return message

You can use the template as follows:

.. code-block:: python

        with dag:
            hello_task = HelloOperator(
                task_id="task_id_1",
                name="{{ task_instance.task_id }}",
                world="Earth",
            )

In this example, Jinja looks for the ``name`` parameter and substitutes ``{{ task_instance.task_id }}`` with
``task_id_1``.


The parameter can also contain a file name, for example, a bash script or a SQL file. You need to add
the extension of your file in ``template_ext``. If a ``template_field`` contains a string ending with
the extension mentioned in ``template_ext``, Jinja reads the content of the file and replace the templates
with actual value. Note that Jinja substitutes the operator attributes and not the args.

.. code-block:: python

        class HelloOperator(BaseOperator):

            template_fields: Sequence[str] = ("guest_name",)
            template_ext = ".sql"

            def __init__(self, name: str, **kwargs) -> None:
                super().__init__(**kwargs)
                self.guest_name = name

In the example, the ``template_fields`` should be ``['guest_name']`` and not  ``['name']``

Additionally you may provide ``template_fields_renderers`` a dictionary which defines in what style the value
from template field renders in Web UI. For example:

.. code-block:: python

        class MyRequestOperator(BaseOperator):
            template_fields: Sequence[str] = ("request_body",)
            template_fields_renderers = {"request_body": "json"}

            def __init__(self, request_body: str, **kwargs) -> None:
                super().__init__(**kwargs)
                self.request_body = request_body

In the situation where ``template_field`` is itself a dictionary, it is also possible to specify a
dot-separated key path to extract and render individual elements appropriately.  For example:

.. code-block:: python

        class MyConfigOperator(BaseOperator):
            template_fields: Sequence[str] = ("configuration",)
            template_fields_renderers = {
                "configuration": "json",
                "configuration.query.sql": "sql",
            }

            def __init__(self, configuration: dict, **kwargs) -> None:
                super().__init__(**kwargs)
                self.configuration = configuration

Then using this template as follows:

.. code-block:: python

        with dag:
            config_task = MyConfigOperator(
                task_id="task_id_1",
                configuration={"query": {"job_id": "123", "sql": "select * from my_table"}},
            )

This will result in the UI rendering ``configuration`` as json in addition to the value contained in the
configuration at ``query.sql`` to be rendered with the SQL lexer.

.. image:: ../img/template_field_renderer_path.png

Currently available lexers:

  - bash
  - bash_command
  - doc
  - doc_json
  - doc_md
  - doc_rst
  - doc_yaml
  - doc_md
  - hql
  - html
  - jinja
  - json
  - md
  - mysql
  - postgresql
  - powershell
  - py
  - python_callable
  - rst
  - sql
  - tsql
  - yaml

If you use a non-existing lexer then the value of the template field will be rendered as a pretty-printed object.

Add template fields with subclassing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
A common use case for creating a custom operator is for simply augmenting existing ``template_fields``.
There might be a situation is which an operator you wish to use doesn't define certain parameters as
templated, but you'd like to be able to dynamically pass an argument as a Jinja expression. This can easily be
achieved with a quick subclassing of the existing operator.

Let's assume you want to use the ``HelloOperator`` defined earlier:

.. code-block:: python

        class HelloOperator(BaseOperator):

            template_fields: Sequence[str] = ("name",)

            def __init__(self, name: str, world: str, **kwargs) -> None:
                super().__init__(**kwargs)
                self.name = name
                self.world = world

            def execute(self, context):
                message = f"Hello {self.world} it's {self.name}!"
                print(message)
                return message

However, you'd like to dynamically parameterize ``world`` arguments. Because the ``template_fields`` property
is guaranteed to be a ``Sequence[str]`` type (i.e. a list or tuple of strings), you can subclass the
``HelloOperator`` to modify the ``template_fields`` as desired easily.

.. code-block:: python

    class MyHelloOperator(HelloOperator):

        template_fields: Sequence[str] = (*HelloOperator.template_fields, "world")

Now you can use ``MyHelloOperator`` like this:

.. code-block:: python

    with dag:
        hello_task = MyHelloOperator(
            task_id="task_id_1",
            name="{{ task_instance.task_id }}",
            world="{{ var.value.my_world }}",
        )

In this example, the ``world`` argument will be dynamically set to the value of an Airflow Variable named
"my_world" via a Jinja expression.


Define an operator extra link
------------------------------

For your operator, you can :doc:`Define an extra link <define-extra-link>` that can
redirect users to external systems. For example, you can add a link that redirects
the user to the operator's manual.

Sensors
-------
Airflow provides a primitive for a special kind of operator, whose purpose is to
poll some state (e.g. presence of a file) on a regular interval until a
success criteria is met.

You can create any sensor your want by extending the :class:`airflow.sensors.base.BaseSensorOperator`
defining a ``poke`` method to poll your external state and evaluate the success criteria.

Sensors have a powerful feature called ``'reschedule'`` mode which allows the sensor to
task to be rescheduled, rather than blocking a worker slot between pokes.
This is useful when you can tolerate a longer poll interval and expect to be
polling for a long time.

Reschedule mode comes with a caveat that your sensor cannot maintain internal state
between rescheduled executions. In this case you should decorate your sensor with
:meth:`airflow.sensors.base.poke_mode_only`. This will let users know
that your sensor is not suitable for use with reschedule mode.

An example of a sensor that keeps internal state and cannot be used with reschedule mode
is :class:`airflow.providers.google.cloud.sensors.gcs.GCSUploadSessionCompleteSensor`.
It polls the number of objects at a prefix (this number is the internal state of the sensor)
and succeeds when there a certain amount of time has passed without the number of objects changing.
