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



Apache Pig Operators
====================

Apache Pig is a platform for analyzing large data sets that consists of a high-level language
for expressing data analysis programs, coupled with infrastructure for evaluating these programs.
Pig programs are amenable to substantial parallelization, which in turns enables them to handle very large data sets.

Use the :class:`~airflow.providers.apache.pig.operators.pig.PigOperator` to execute a pig script.

.. exampleinclude:: /../../providers/tests/system/apache/pig/example_pig.py
    :language: python
    :start-after: [START create_pig]
    :end-before: [END create_pig]
