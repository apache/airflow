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


Create Custom Operator
=======================


Airflow allows you to create new operators to suit the requirements of you or your team. 
The extensibility is one of the many reasons which makes Apache Airflow powerful. 

You can create any operator you want by extending the :class:`airflow.models.baseoperator.BaseOperator`

There are two methods that you need to override in derived class:

* Constructor - Define the parameters required for the operator. You only need to specify the arguments specific to your operator.

* Execute - The code to execute when the runner calls the operator. The method contains the 
  airflow context as a parameter that can be used to read config values.

Let's implement an example ``HelloOperator``:

.. code::  python

        class HelloOperator(BaseOperator):

            def __init__(
                    self,
                    name: str,
                    *args, **kwargs) -> None:
                super().__init__(*args, **kwargs)
                self.name = name

            def execute(self, context):
                message = "Hello {}".format(name)
                print(message)
                return message

The above operator can now be used as follows:

.. code:: python

    hello_task = HelloOperator(task_id='', dag=dag, name='foo_bar')

Hooks
^^^^^
Hooks act as an interface to communicate with the external shared resources in a DAG.
For example, multiple tasks in a DAG can require access to a MySQL database. Instead of
creating a connection per task, you can retrieve a connection from the hook and utilize it.
Hook also helps to avoid storing connection auth parameters in a DAG. 
See :doc:`../connection/index` for how to create and manage connections.

Let's extend our previous example to fetch name from MySQL:

.. code:: python

    class HelloDBOperator(BaseOperator):

            def __init__(
                    self,
                    name: str,
                    conn_id: str,
                    database: str,
                    *args, **kwargs) -> None:
                super().__init__(*args, **kwargs)
                self.name = name
                self.conn_id = conn_id
                self.database = database

            def execute(self, context):
                hook = MySqlHook(mysql_conn_id=self.mysql_conn_id,
                         schema=self.database)
                sql = "select name from user"
                result = hook.get_first(sql)
                message = "Hello {}".format(result['name'])
                print(message)
                return message

When the operator invokes the query on the hook object, a new connection gets created if it doesn't exist. 
The hook retrieves the auth parameters such as username and password from Airflow
backend and passes the params to the :py:func:`airflow.hooks.base_hook.BaseHook.get_connection`. 


User interface
^^^^^^^^^^^^^^^
Airflow also allows the developer to control how the operator shows up in the DAG UI.
Override ``ui_color``to change the background color of the operator in UI. 
Override ``ui_fgcolor`` to change the color of the label.

.. code::  python

        class HelloOperator(BaseOperator):
            ui_color = '#ff0000'
            ui_fgcolor = '#000000'
            ....

Templating
^^^^^^^^^^^
You can use :ref:`Jinja templates <jinja-templating>` to parameterize your operator.
Airflow considers the field names present in ``template_fields``  for templating while rendering
the operator.

.. code:: python
    
        class HelloOperator(BaseOperator):
            
            template_fields = ['name']

            def __init__(
                    self,
                    name: str,
                    *args, **kwargs) -> None:
                super().__init__(*args, **kwargs)
                self.name = name

            def execute(self, context):
                message = "Hello from {}".format(name)
                print(message)
                return message

        hello_task = HelloOperator(task_id='task_id_1', dag=dag, name='{{ task_id }}')

In this example, Jinja looks for the ``name`` parameter and substitutes ``{{ task_id }}`` with
task_id_1 .

The parameter can also contain a file name, for example, a bash script or a SQL file. You need to add
the extension of your file in ``template_ext``. If a ``template_field`` contains a string ending with
the extension mentioned in ``template_ext``, Jinja reads the content of the file and replace the templates
with actual value.


Define an operator extra link
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For your operator, you can define extra links that can
redirect users to external systems. The extra link buttons
will be available on the task page:

.. image:: ../../img/operator_extra_link.png

You should override ``operator_extra_link_dict`` parameter with the links. The following code shows how to add link to the ``HelloOperator``:

.. code-block:: python

    from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
    from airflow.utils.decorators import apply_defaults


    class GoogleLink(BaseOperatorLink):

        def get_link(self, operator, dttm):
            return "https://www.google.com"

    class HelloOperator(BaseOperator):

        operator_extra_link_dict = {
            "Google": GoogleLink(),
        }
        ...

You can also add a global operator extra link that will be available to
all the operators through airflow plugin. Learn more about it in the
:ref:`plugin example <plugin-example>`.
