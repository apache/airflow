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


.. NOTE TO CONTRIBUTORS:
   Please, only add notes to the Changelog just below the "Changelog" header when there are some breaking changes
   and you want to add an explanation to the users on how they are supposed to deal with them.
   The changelog is updated and maintained semi-automatically by release manager.

``apache-airflow-providers-oracle-oracledb``


Changelog
---------

4.6.3
.....

This is the first release of ``apache-airflow-providers-oracle-oracledb``. It is extracted
from ``apache-airflow-providers-oracle``, whose ``4.6.3`` release deprecates the
``airflow.providers.oracle`` python package in favor of this one.

All classes moved unchanged (same behavior, same connection type ``oracle``, same
``oracle_default`` connection id) to their new import paths:

==============================================================================  ============================================================================
Old import (``airflow.providers.oracle``, deprecated)                          New import (``airflow.providers.oracle.oracledb``)
==============================================================================  ============================================================================
``airflow.providers.oracle.hooks.oracle.OracleHook``                           ``airflow.providers.oracle.oracledb.hooks.oracle.OracleHook``
``airflow.providers.oracle.hooks.handlers``                                    ``airflow.providers.oracle.oracledb.hooks.handlers``
``airflow.providers.oracle.operators.oracle.OracleStoredProcedureOperator``    ``airflow.providers.oracle.oracledb.operators.oracle.OracleStoredProcedureOperator``
``airflow.providers.oracle.transfers.oracle_to_oracle.OracleToOracleOperator`` ``airflow.providers.oracle.oracledb.transfers.oracle_to_oracle.OracleToOracleOperator``
``airflow.providers.oracle.assets.oracle``                                     ``airflow.providers.oracle.oracledb.assets.oracle``
==============================================================================  ============================================================================

To migrate, replace ``apache-airflow-providers-oracle`` with
``apache-airflow-providers-oracle-oracledb`` in your dependencies and update the imports
above in your Dags and plugins. The old import paths keep working for now but emit a
deprecation warning and will be removed in a future release.
