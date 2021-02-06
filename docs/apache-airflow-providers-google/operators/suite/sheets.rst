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

Google Sheets Operators
=======================

The latest version of the Sheets API lets developers programmatically:

- Read and write data
- Format text and numbers
- Build pivot tables
- Enforce cell validation
- Set frozen rows
- Adjust column sizes
- Apply formulas
- Create charts... and more!

For more information check `official documentation <https://developers.google.com/sheets/api>`__.


.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include::/operators/_partials/prerequisite_tasks.rst

.. _howto/operator:GoogleSheetsCreateSpreadsheetOperator:

Create spreadsheet
^^^^^^^^^^^^^^^^^^

To create new spreadsheet you can use the
:class:`~airflow.providers.google.suite.operators.sheets.GoogleSheetsCreateSpreadsheetOperator`.

.. exampleinclude:: /../../airflow/providers/google/suite/example_dags/example_sheets.py
    :language: python
    :dedent: 4
    :start-after: [START create_spreadsheet]
    :end-before: [END create_spreadsheet]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.suite.operators.sheets.GoogleSheetsCreateSpreadsheetOperator`.

To get the URL of newly created spreadsheet use XCom value:

.. exampleinclude:: /../../airflow/providers/google/suite/example_dags/example_sheets.py
    :language: python
    :dedent: 4
    :start-after: [START print_spreadsheet_url]
    :end-before: [END print_spreadsheet_url]
