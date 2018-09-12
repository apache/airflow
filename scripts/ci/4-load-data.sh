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

set -exuo pipefail

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
DATA_DIR="${DIR}/data"
DATA_FILE="${DATA_DIR}/baby_names.csv"
DATABASE=airflow_ci
HOST=mysql

mysqladmin -h ${HOST} -u root create ${DATABASE}
mysql -h ${HOST} -u root < ${DATA_DIR}/mysql_schema.sql
mysqlimport --local -h ${HOST} -u root --fields-optionally-enclosed-by="\"" --fields-terminated-by=, --ignore-lines=1 ${DATABASE} ${DATA_FILE}
