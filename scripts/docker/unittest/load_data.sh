#!/usr/bin/env bash
#  Licensed to the Apache Software Foundation (ASF) under one   *
#  or more contributor license agreements.  See the NOTICE file *
#  distributed with this work for additional information        *
#  regarding copyright ownership.  The ASF licenses this file   *
#  to you under the Apache License, Version 2.0 (the            *
#  "License"); you may not use this file except in compliance   *
#  with the License.  You may obtain a copy of the License at   *
#                                                               *
#    http://www.apache.org/licenses/LICENSE-2.0                 *
#                                                               *
#  Unless required by applicable law or agreed to in writing,   *
#  software distributed under the License is distributed on an  *
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY       *
#  KIND, either express or implied.  See the License for the    *
#  specific language governing permissions and limitations      *
#  under the License.                                           *

mysql -u root -proot -e "CREATE SCHEMA IF NOT EXISTS airflow_ci"
mysql -u root -proot -e "CREATE TABLE IF NOT EXISTS airflow_ci.baby_names (org_year integer(4), baby_name VARCHAR(25), rate FLOAT(7,6),sex VARCHAR(4));"
mysqlimport -u root -proot --fields-optionally-enclosed-by="\"" --fields-terminated-by=, --ignore-lines=1 airflow_ci /var/lib/mysql-files/baby_names.csv
