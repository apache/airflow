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
from hooks.teradata import TeradataHook

tdh = TeradataHook()
tdh.test_connection()
conn = tdh.get_conn()
conn_uri_str = tdh.get_uri()
cur = tdh.get_cursor()

res = tdh.run(sql=[
    "INSERT INTO my_users(user_name) VALUES ('User1');",
    "INSERT INTO my_users(user_name) VALUES ('User2');",
    "INSERT INTO my_users(user_name) VALUES ('User3');",
])
rows = [('User5',), ('User6',)]
target_fields = ["user_name"]
res = tdh.insert_rows(table="my_users", rows=rows, target_fields=target_fields)

recs = tdh.get_records(sql="select * from airbyte_td.my_users;")


df = tdh.get_pandas_df("select top 10 _airbyte_ab_id from airbyte_td._airbyte_raw_Sales;")
gen = tdh.get_pandas_df_by_chunks(sql="select top 10 _airbyte_ab_id from airbyte_td._airbyte_raw_Sales;", chunksize=2)
tdh.strip_sql_string(sql="select top 10 _airbyte_ab_id from airbyte_td._airbyte_raw_Sales;")
