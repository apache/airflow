#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from datetime import datetime
from unittest import TestCase

from airflow import DAG

DEFAULT_DATE = datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]


class TestHiveEnvironment(TestCase):

    def setUp(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        dag = DAG('test_dag_id', default_args=args)
        self.dag = dag
        self.hql = """
        USE airflow;
        DROP TABLE IF EXISTS static_babynames_partitioned;
        CREATE TABLE IF NOT EXISTS static_babynames_partitioned (
            state string,
            year string,
            name string,
            gender string,
            num int)
        PARTITIONED BY (ds string);
        INSERT OVERWRITE TABLE static_babynames_partitioned
            PARTITION(ds='{{ ds }}')
        SELECT state, year, name, gender, num FROM static_babynames;
        """
