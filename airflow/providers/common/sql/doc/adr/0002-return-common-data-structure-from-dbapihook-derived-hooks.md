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

# 2. Return common data structure from DBApiHook derived hooks

Date: 2023-12-01

## Status

Accepted

## Context

Note: This ADR describes the decision made (but not recorded) when common.sql provider had been
introduced in July 2022, but this ADR is recorded in December 2023 to make sure the decision is
recorded.

Before common.sql provider, we had a number of DBAPI-derived Hooks which interfaced with Python
DBAPI-compliant database and the format of data returned from these hooks were different and
dependent on the implementation of the DBAPI interface as well as implementation of the Hooks and operators.
We also had a lot of very similar operators that performed similar tasks - like Querying, Sensing the
database, column check etc. This led to a lot of code duplication, and we decided that we need a common set
of operators that can be used across all the DBAPI-compliant databases.

Unfortunately there is no common standard for data returned by Python DBAPI interface. It's partially
standardized by [PEP-0249](https://peps.python.org/pep-0249/), but the specification is not very
strict and it allows for a lot of flexibility and interpretation. Consequently, the data returned
by the DBAPI interface can contain tuples, named tuples, lists, dictionaries, or even custom objects and
there are no guarantees that the data returned by the DBAPI interface is directly serializable.

Not having a common standard format made it difficult to implement planned open-lineage column-level
integration with the database Operators and - in later stage Hooks.

## Decision

We decided to introduce a common.sql provider that would contain a set of operators that can be used
across all the DBAPI-compliant databases. We also decided that the returned data structure from the
operators should be consistent across all the operators. For simplicity of transition we chose the format
returned by the `run` method of the ``DBApiHook`` class that was very close to what most of the
DBAPI-compliant Hooks already returned, even if it was not the most optimal format. In this case
backwards compatibility trumped the optimal format.

The decision has been made that more optimal formats (possibly some form of DataFrames) might be
introduced in the future if we find a need for that. However, this is likely not even needed in the
future because Pandas and similar libraries already have excellent support for converting many of
the formats returned by the DBAPI interface to DataFrames and we are already leveraging those
by directly using Pandas functionality via `get_pands_df` method of the ``DBApiHook`` class

The goal of the change was to standardize the format of the data returned that could be
directly used through existing DBAPI Airflow operators, with minimal  backwards-compatibility problems,
and resulting in deprecating and redirecting all the existing DBAPI operators to the new operators
defined in the common.sql provider.

Therefore, the format of data returned by the Hook can be one of:

* base return value is a list of tuples where each tuple is a row returned by the query. The tuples
  are not named tuples, they should be directly serializable and they should not contain
  column metadata like column names.
* a tuple (same properties as above) - in case the query returns a single row -
  based on parameters passed (e.g. `return_last=True`)
* it can also be a list of list of tuples in case the operator receives multiple queries to execute,
  in which case the return value is a list of list of tuples, where each list of tuples is a result
  of a single query. This is also the default format when ``split_statements`` parameter is set to
  ``True`` indicating the query passed contains multiple statements and it's up to the implementation
  of the hook to split the statements and execute them separately.
* None - in case the run method is used to execute a query that does not return any rows (e.g. `INSERT`)

Additionally, it has been agreed to that description returned by the `run` method should be stored in
the ``descriptions`` field of the Hook - separately from the returned data. This is because the description
is not always available in the DBAPI interface, and it's not always available in the same format - the
format of ``descriptions`` field is that it is always an array of 7-element tuples described in PEP-0249 -
each element of the array correspond to a single query passed to the hook. There is also a separate
``last_description`` property that contains the description of the last query passed to the hook,
particularly, the only query if there was only one.

The DBApiHook implements the base `run` method that returns the list of tuples directly from the DBAPI
result, provide. The `run` method is extendable - it can receive handlers that can modify the data
returned by the direct DBAPI calls to the common format.

For backwards compatibility, the Hooks could implement their own parameters that should allow to return
the "old" format of the data. For example in SnowflakeHook we have `return_dict` parameter that allows
to return the data as a list of dictionaries instead of a list of tuples. This allowed to keep
easy backwards compatibility with the existing operators that our users already had, and allowed for
an easy, even gradual transition to new common format if the user would see the benefit of doing so
(for example to support column based open-lineage or simplify the usage of deprecated operators
and switch to the new ones from the old, deprecated specialized operators.

We also decided that the common.sql provider implements the operators that can instantiate the Hooks
based on the connection type/URI and execute the commands using that hook, while deprecating the use
of the specialized ones:

The new operators are:

* ``SQLExecuteQueryOperator`` - to execute SQL queries
* ``SQLColumnCheckOperator`` - to perform various checks (declarative) on columns in the database and
  return matching rows
* ``SQLTableCheckOperator`` - to perform various checks (declarative) on tables in the database and
  return calculated results (usually aggregated calculations)
* ``SQLCheckOperator`` - to perform various checks (declarative) on the database and return success/failure
  based on the checks performed
* ``SQLValueCheckOperator`` - to perform various checks (declarative) on values in the database and
  return matching rows
* ``SQLIntervalCheckOperator`` - to perform various checks (declarative) on intervals in the database and
  return matching rows
* ``BranchSQLOperator``- branch operator where branching is based on sql and parameters provided
* ``SQLSensor`` - sensor that waits for a certain condition to be met in the database


## Consequences

The consequence of this decision is that the data returned by the operators in the common.sql provider
is consistent across all the operators and it's easy to use it for various databases in similar way
and easy to integrate with other components like open-lineage. The data returned by the operators
is not the most optimal format for all the databases, but it's a good compromise that allows for
easy transition and backwards compatibility with the existing operators.

Column-lineage integration is possible with the data returned by the operators in the common.sql
provider as well as returning the data directly by the hook to be stored in XCom or other ways SQL output
can be serialized to (for example CSV records).

A number of SQL/DBAPI operators implemented is therefore much smaller and the code to maintain is
only present in the common.sql provider. The code is also much more consistent and easier to maintain
and extend.
