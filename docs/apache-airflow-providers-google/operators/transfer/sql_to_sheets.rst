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

SQL to Google Sheets Transfer Operators
========================================================

With `Google Sheets <https://www.google.pl/intl/en/sheets/about/>`__, everyone can work together in the same
spreadsheet at the same time. Use formulas functions, and formatting options to save time and simplify
common spreadsheet tasks.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:SQLToGoogleSheets:

Upload data from SQL to Google Sheets
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To upload data from and Database using SQL to Google Spreadsheet you can use the
:class:`~airflow.providers.google.suite.transfers.sql_to_sheets.SQLToGoogleSheetsOperator`.

.. exampleinclude:: /../../providers/tests/system/google/cloud/sql_to_sheets/example_sql_to_sheets.py
    :language: python
    :dedent: 4
    :start-after: [START upload_sql_to_sheets]
    :end-before: [END upload_sql_to_sheets]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.suite.transfers.sql_to_sheets.SQLToGoogleSheetsOperator`.
