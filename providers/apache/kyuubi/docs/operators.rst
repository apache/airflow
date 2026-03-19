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

.. _howto/operator:KyuubiOperator:

Apache Kyuubi Operator
======================

Use the :class:`~airflow.providers.apache.kyuubi.operators.kyuubi.KyuubiOperator` to execute
Hive Query Language (HQL) statements against Kyuubi.

Using the Operator
^^^^^^^^^^^^^^^^^^

.. code-block:: python

    from airflow.providers.apache.kyuubi.operators.kyuubi import KyuubiOperator

    t1 = KyuubiOperator(
        task_id="kyuubi_task",
        hql="SELECT * FROM table",
        kyuubi_conn_id="kyuubi_default",
        spark_queue="root.default",
        spark_app_name="my_kyuubi_app",
        spark_conf={"spark.executor.memory": "4g"},
    )
