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



.. _howto/decorator:pyspark:

PySpark Decorator
=================

Python callable wrapped within the ``@task.pyspark`` decorator
is injected with a SparkContext object.

Parameters
----------

The following parameters can be passed to the decorator:

conn_id: str
    The connection ID to use for connecting to the Spark cluster. If not
    specified, the spark master is set to ``local[*]``.
config_kwargs: dict
    The kwargs used for initializing the SparkConf object. This overrides
    the spark configuration options set in the connection.


Example
-------

The following example shows how to use the ``@task.pyspark`` decorator. Note
that the ``spark`` and ``sc`` objects are injected into the function.

.. exampleinclude:: /../../tests/system/providers/apache/spark/example_pyspark.py
    :language: python
    :dedent: 4
    :start-after: [START task_pyspark]
    :end-before: [END task_pyspark]
