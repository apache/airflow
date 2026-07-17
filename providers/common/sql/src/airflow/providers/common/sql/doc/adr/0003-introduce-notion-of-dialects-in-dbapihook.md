<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# 3. Introduce notion of dialects in DbApiHook

Date: 2025-01-07

## Status

Accepted

## Context

This ADR describes the proposition why we wanted to introduce dialects in the ``DBAPIHook`` as we experienced
that the ``_insert_statement_format`` and ``_replace_statement_format`` string formatting properties used by the
``insert_rows`` method in the ``DbApiHook`` where lacking in some cases as the number of parameters passed to the
string format are hard-coded and aren't always sufficient when using different database through the
generic JBDC and ODBC connection types.

That's why we wanted a generic approach in which the code isn't tied to a specific database hook.

For example when using MsSQL through ODBC instead of the native ``MsSqlHook``, you won't have the merge into
(e.g. replace) functionality for MSSQL when using the ODBC connection type as that one was only available in
the native ``MsSqlHook``.

That's where the notion of dialects come into play and allow us to benefit of the same functionalities
independently of which connection type you want to use (ODBC/JDBC or native if available) for a specific
database.


## Decision

We decided the introduce the notion of dialects which allows us to implement database specific functionalities
independently of the used connection type (e.g. hook).  That way when using for example the ``insert_rows`` method on
the ``DbApiHook`` for as well ODBC as JDBC as native connection types, it will always be possible to use the replace
into (e.g. merge into) functionality as that won't be tied to a specific implementation with a Hook an thus the
connection type.


## Consequences

The consequence of this decision is that from now on database specific implementations should be done within the
dialect for that database instead of the specialized hook, unless the connection type is tied to the hook,
meaning that there is only one connection type possible and an ODBC/JDBC and in the future maybe even ADBC
(e.g. Apache Arrow) isn't available.
