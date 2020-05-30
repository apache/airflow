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

## Changelog

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [v2020.XX.XX](#v2020xxxx)
  - [New operators](#new-operators)
  - [Updated operators](#updated-operators)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### v2020.XX.XX

This is the first released version of the package.

#### New operators

This release includes one new operator, `S3ToSnowflakeTransfer`.

#### Updated operators

The operators in Airflow 2.0 have been moved to a new package. The following table showing operators
from Airflow 1.10 and its equivalent from Airflow 2.0:

| Airflow 1.10  (`airflow.contrib.operators` package)                                      | Airflow 2.0 (`airflow.providers.snowflake.operators` package)                  |
|------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|
| snowflake_operator.SnowflakeOperator                                                     | snowflake.SnowflakeOperator                                                    |
