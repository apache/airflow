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

TaskFlow
========

.. versionadded:: 2.0

If you write most of your DAGs using plain Python code rather than Operators, then the TaskFlow API will make it much easier to author clean DAGs without extra boilerplate, all using the ``@task`` decorator.

TaskFlow takes care of moving inputs and outputs between your Tasks using XComs for you, as well as automatically calculating dependencies - when you call a TaskFlow function in your DAG file, rather than executing it, you will get an object representing the XCom for the result (an ``XComArg``), that you can then use as inputs to downstream tasks or operators. For example::

    from airflow.decorators import task
    from airflow.operators.email import EmailOperator

    @task
    def get_ip():
        return my_ip_service.get_main_ip()

    @task(multiple_outputs=True)
    def compose_email(external_ip):
        return {
            'subject':f'Server connected from {external_ip}',
            'body': f'Your server executing Airflow is connected from the external IP {external_ip}<br>'
        }

    email_info = compose_email(get_ip())

    EmailOperator(
        task_id='send_email_notification',
        to='example@example.com',
        subject=email_info['subject'],
        html_content=email_info['body']
    )

Here, there are three tasks - ``get_ip``, ``compose_email``, and ``send_email_notification``.

The first two are declared using TaskFlow, and automatically pass the return value of ``get_ip`` into ``compose_email``, not only linking the XCom across, but automatically declaring that ``compose_email`` is *downstream* of ``get_ip``.

``send_email_notification`` is a more traditional Operator, but even it can use the return value of ``compose_email`` to set its parameters, and again, automatically work out that it must be *downstream* of ``compose_email``.

You can also use a plain value or variable to call a TaskFlow function - for example, this will work as you expect (but, of course, won't run the code inside the task until the DAG is executed - the ``name`` value is persisted as a task parameter until that time)::

    @task
    def hello_name(name: str):
        print(f'Hello {name}!')

    hello_name('Airflow users')

If you want to learn more about using TaskFlow, you should consult :doc:`the TaskFlow tutorial </tutorial/taskflow>`.

Context
-------

You can access Airflow :ref:`context variables <templates:variables>` by adding them as keyword arguments as shown in the following example:

.. include:: ../../shared/template-examples/taskflow.rst

Alternatively, you may add ``**kwargs`` to the signature of your task and all Airflow context variables will be accessible in the ``kwargs`` dict:

.. include:: ../../shared/template-examples/taskflow-kwargs.rst

For a full list of context variables, see :ref:`context variables <templates:variables>`.


Logging
-------

To use logging from your task functions, simply import and use Python's logging system:

.. code-block:: python

   logger = logging.getLogger("airflow.task")

Every logging line created this way will be recorded in the task log.

.. _concepts:arbitrary-arguments:

Passing Arbitrary Objects As Arguments
--------------------------------------

.. versionadded:: 2.5.0

As mentioned TaskFlow uses XCom to pass variables to each task. This requires that variables that are used as arguments
need to be able to be serialized. Airflow out of the box supports all built-in types (like int or str) and it
supports objects that are decorated with ``@dataclass`` or ``@attr.define``. The following example shows the use of
a ``Dataset``, which is ``@attr.define`` decorated, together with TaskFlow.

.. note::

    An additional benefit of using ``Dataset`` is that it automatically registers as an ``inlet`` in case it is used as an input argument. It also auto registers as an ``outlet`` if the return value of your task is a ``dataset`` or a ``list[Dataset]]``.


.. code-block:: python

    import json
    import pendulum
    import requests

    from airflow import Dataset
    from airflow.decorators import dag, task

    SRC = Dataset(
        "https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/global/time-series/globe/land_ocean/ytd/12/1880-2022.json"
    )
    now = pendulum.now()


    @dag(start_date=now, schedule="@daily", catchup=False)
    def etl():
        @task()
        def retrieve(src: Dataset) -> dict:
            resp = requests.get(url=src.uri)
            data = resp.json()
            return data["data"]

        @task()
        def to_fahrenheit(temps: dict[int, float]) -> dict[int, float]:
            ret: dict[int, float] = {}
            for year, celsius in temps.items():
                ret[year] = float(celsius) * 1.8 + 32

            return ret

        @task()
        def load(fahrenheit: dict[int, float]) -> Dataset:
            filename = "/tmp/fahrenheit.json"
            s = json.dumps(fahrenheit)
            f = open(filename, "w")
            f.write(s)
            f.close()

            return Dataset(f"file:///{filename}")

        data = retrieve(SRC)
        fahrenheit = to_fahrenheit(data)
        load(fahrenheit)


    etl()

Custom Objects
^^^^^^^^^^^^^^
It could be that you would like to pass custom objects. Typically you would decorate your classes with ``@dataclass`` or
``@attr.define`` and Airflow will figure out what it needs to do. Sometime you might want to control serialization
yourself. To do so add the ``serialize()`` method to your class and the staticmethod
``deserialize(data: dict, version: int)`` to your class. Like so:

.. code-block:: python

    from typing import ClassVar


    class MyCustom:
        __version__: ClassVar[int] = 1

        def __init__(self, x):
            self.x = x

        def serialize(self) -> dict:
            return dict({"x": self.x})

        @staticmethod
        def deserialize(data: dict, version: int):
            if version > 1:
                raise TypeError(f"version > {MyCustom.version}")
            return MyCustom(data["x"])

Object Versioning
^^^^^^^^^^^^^^^^^

It is good practice to version the objects that will be used in serialization. To do this add
``__version__: ClassVar[int] = <x>`` to your class. Airflow assumes that your classes are backwards compatible,
so that a version 2 is able to deserialize a version 1. In case you need custom logic
for deserialization ensure that ``deserialize(data: dict, version: int)`` is specified.

.. note::

  Typing of ``__version__`` is required and needs to be ``ClassVar[int]``


Sensors and the TaskFlow API
--------------------------------------

.. versionadded:: 2.5.0

For an example of writing a Sensor using the TaskFlow API, see
:ref:`Using the TaskFlow API with Sensor operators <taskflow/task_sensor_example>`.

History
-------

The TaskFlow API is new as of Airflow 2.0, and you are likely to encounter DAGs written for previous versions of Airflow that instead use ``PythonOperator`` to achieve similar goals, albeit with a lot more code.

More context around the addition and design of the TaskFlow API can be found as part of its Airflow Improvement Proposal
`AIP-31: "TaskFlow API" for clearer/simpler DAG definition <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=148638736>`_
