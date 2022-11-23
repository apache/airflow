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



.. _howto/operator:CdwExecuteQueryOperator:


CdwExecuteQueryOperator
=======================

Use the :class:`~airflow.providers.cloudera.operators.CdwExecuteQueryOperator` to execute hql code in CDW.


Using the Operator
------------------

Executes hql code in CDW. This class inherits behavior from HiveOperator, and instantiates a CdwHook to do the work.


.. list-table::
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - schema: str
     - The name of the DB schema, default value ``'default'``
   * - hiveconfs: dict
     - An optional dictionary of key-value pairs to define hive configurations
   * - hiveconf_jinja_translate: bool
     - default value ``False``.
   * - cli_conn_id: str
     - The Airflow connection id for the target CDW instance, default value ``'hive_cli_default'``
   * - jdbc_driver: str
     - Package name of the Impala jdbc_driver, for instance "com.cloudera.impala.jdbc41.Driver". Required for Impala connections. None by default.
   * - query_isolation: bool
     - Controls whether to use cdw's query isolation feature. Only hive warehouses support this at the moment. Default ``True``.
