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

from __future__ import annotations

from airflow.providers.cloudera.hooks.cdw_hook import CdwHiveMetastoreHook


def test_csv_parse():
    """
    This is just simple validation test for csv reader. The variable beeline_output
    contains a sample response which comes from hive in case of --outputformat=csv2.
    """
    beeline_output = "db_name,tbl_name,part_name\ndefault,test_part,dt=1"
    result_list = CdwHiveMetastoreHook.parse_csv_lines(beeline_output)

    assert len(result_list) == 2, result_list
