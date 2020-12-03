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

The following table shows the list of alembic revisions


.. list-table:: Alembic Revisions(until Airflow 2.0.0):
   :widths: 25 25 50
   :header-rows: 1

   * - Revision ID
     - Revises ID
     - Airflow Version
     - Summary
   * - e3a246e0dc1
     - 1507a7289a2f
     - 2.0.0
     - create is_encrypted
   * - 1507a7289a2f
     - 13eb55f81627
     - 2.0.0
     - maintain history for compatibility with earlier migrations
   * - 13eb55f81627
     - 338e90f54d61
     - 2.0.0
     - More logging into task_instance
   * - 338e90f54d61
     - 52d714495f0
     - 2.0.0
     - job_id indices
   * - 52d714495f0
     - 502898887f84
     - 2.0.0
     - Adding extra to Log
   * - 502898887f84
     - 1b38cef5b76e
     - 2.0.0
     - add dagrun
   * - 1b38cef5b76e
     - 2e541a1dcfed
     - 2.0.0
     - task_duration
   * - 2e541a1dcfed
     - 40e67319e3a9
     - 2.0.0
     - dagrun_config
   * - 2e541a1dcfed
     - 40e67319e3a9
     - 2.0.0
     - task_duration
   * - 40e67319e3a9
     - 561833c1c74b
     - 2.0.0
     - add password column to user
   * - 561833c1c74b
     - 4446e08588
     - 2.0.0
     - dagrun start end
   * - 4446e08588
     - bbc73705a13e
     - 2.0.0
     - Add notification_sent column to sla_miss
   * - bbc73705a13e
     - bba5a7cfc896
     - 2.0.0
     - Add a column to track the encryption state of the 'Extra' field in connection
   * - bba5a7cfc896
     - 1968acfc09e3
     - 2.0.0
     - add is_encrypted column to variable table
   * - 1968acfc09e3
     - 2e82aab8ef20
     - 2.0.0
     - rename user table
   * - 2e82aab8ef20
     - 211e584da130
     - 2.0.0
     - add TI state index
   * - 211e584da130
     - 64de9cddf6c9
     - 2.0.0
     - add task fails journal table
   * - 64de9cddf6c9
     - f2ca10b85618
     - 2.0.0
     - add dag_stats table
   * - f2ca10b85618
     - 4addfa1236f1
     - 2.0.0
     - Add fractional seconds to mysql tables
   * - 4addfa1236f1
     - 8504051e801b
     - 2.0.0
     -  xcom dag task indices
   * - 8504051e801b
     - 5e7d17757c7a
     - 2.0.0
     - add pid field to TaskInstance
   * - 5e7d17757c7a
     - 127d2bf2dfa7
     - 2.0.0
     - Add dag_id/state index on dag_run table
   * - 127d2bf2dfa7
     - cc1e65623dc7
     - 2.0.0
     - add max tries column to task instance
   * - cc1e65623dc7
     - bdaa763e6c56
     - 2.0.0
     - Make xcom value column a large binary
   * - bdaa763e6c56
     - 947454bf1dff
     - 2.0.0
     - add ti job_id index
   * - 947454bf1dff
     - d2ae31099d61
     - 2.0.0
     - Increase text size for MySQL (not relevant for other DBs' text types)
   * - d2ae31099d61
     - 0e2a74e0fc9f
     - 2.0.0
     - Add time zone awareness
   * - d2ae31099d61
     - 33ae817a1ff4
     - 2.0.0
     - kubernetes_resource_checkpointing
   * - 33ae817a1ff4
     - 27c6a30d7c24
     - 2.0.0
     - kubernetes_resource_checkpointing
   * - 27c6a30d7c24
     - 86770d1215c0
     - 2.0.0
     - add kubernetes scheduler uniqueness
   * - 86770d1215c0, 0e2a74e0fc9f
     - 05f30312d566
     - 2.0.0
     - merge heads
   * - 05f30312d566
     - f23433877c24
     - 2.0.0
     - fix mysql not null constraint
   * - f23433877c24
     - 856955da8476
     - 2.0.0
     - fix sqlite foreign key
   * - 856955da8476
     - 9635ae0956e7
     - 2.0.0
     - index-faskfail
   * - 9635ae0956e7
     - dd25f486b8ea
     - 2.0.0
     - add idx_log_dag
   * - dd25f486b8ea
     - bf00311e1990
     - 2.0.0
     - add index to taskinstance
   * - 9635ae0956e7
     - 0a2a5b66e19d
     - 2.0.0
     - add task_reschedule table
   * - 0a2a5b66e19d, bf00311e1990
     - 03bc53e68815
     - 2.0.0
     - merge_heads_2
   * - 03bc53e68815
     - 41f5f12752f8
     - 2.0.0
     - add superuser field
   * - 41f5f12752f8
     - c8ffec048a3b
     - 2.0.0
     - add fields to dag
   * - c8ffec048a3b
     - dd4ecb8fbee3
     - 2.0.0
     - Add schedule interval to dag
   * - dd4ecb8fbee3
     - 939bb1e647c8
     - 2.0.0
     - task reschedule fk on cascade delete
   * - 939bb1e647c8
     - 6e96a59344a4
     - 2.0.0
     - Make TaskInstance.pool not nullable
   * - 6e96a59344a4
     - d38e04c12aa2
     - 2.0.0
     - add serialized_dag table
   * - d38e04c12aa2
     - b3b105409875
     - 2.0.0
     - add root_dag_id to DAG
   * - 6e96a59344a4
     - 74effc47d867
     - 2.0.0
     - change datetime to datetime2(6) on MSSQL tables
   * - 939bb1e647c8
     - 004c1210f153
     - 2.0.0
     - increase queue name size limit
   * - c8ffec048a3b
     - a56c9515abdc
     - 2.0.0
     - Remove dag_stat table
   * - a56c9515abdc, 004c1210f153, 74effc47d867, b3b105409875
     - 08364691d074
     - 2.0.0
     - Merge the four heads back together
   * - 08364691d074
     - fe461863935f
     - 2.0.0
     - increase_length_for_connection_password
   * - fe461863935f
     - 7939bcff74ba
     - 2.0.0
     - Add DagTags table
   * - 7939bcff74ba
     - a4c2fd67d16b
     - 2.0.0
     - add pool_slots field to task_instance
   * - a4c2fd67d16b
     - 852ae6c715af
     - 2.0.0
     - Add RenderedTaskInstanceFields table
   * - 852ae6c715af
     - 952da73b5eff
     - 2.0.0
     - add dag_code table
   * - 952da73b5eff
     - a66efa278eea
     - 2.0.0
     - Add Precision to execution_date in RenderedTaskInstanceFields table
   * - a66efa278eea
     - cf5dc11e79ad
     - 2.0.0
     - drop_user_and_chart
   * - cf5dc11e79ad
     - bbf4a7ad0465
     - 2.0.0
     - Remove id column from xcom
   * - bbf4a7ad0465
     - b25a55525161
     - 2.0.0
     - Increase length of pool name
   * - b25a55525161
     - 3c20cacc0044
     - 2.0.0
     - Add DagRun run_type
   * - 3c20cacc0044
     - 8f966b9c467a
     - 2.0.0
     - Set conn_type as non-nullable
   * - 8f966b9c467a
     - 8d48763f6d53
     - 2.0.0
     - add unique constraint to conn_id
   * - 8d48763f6d53
     - da3f683c3a5a
     - 2.0.0
     - Add dag_hash Column to serialized_dag table
   * - da3f683c3a5a
     - e38be357a868
     - 2.0.0
     - Add sensor_instance table
   * - e38be357a868
     - b247b1e3d1ed
     - 2.0.0
     - Add queued by Job ID to TI
   * - b247b1e3d1ed
     - e1a11ece99cc
     - 2.0.0
     - Add external executor ID to TI
   * - e1a11ece99cc
     - bef4f3d11e8b
     - 2.0.0
     - Drop KubeResourceVersion and KubeWorkerId
   * - bef4f3d11e8b
     - 98271e7606e2
     - 2.0.0
     - Add scheduling_decision to DagRun and DAG
   * - 98271e7606e2
     - 52d53670a240
     - 2.0.0
     - fix_mssql_exec_date_rendered_task_instance_fields_for_MSSQL
   * - 52d53670a240
     - 364159666cbd
     - 2.0.0
     - Add creating_job_id to DagRun table
   * - 364159666cbd
     - 45ba3f1493b9
     - 2.0.0
     - add-k8s-yaml-to-rendered-templates
   * - 45ba3f1493b9
     - 92c57b58940d
     - 2.0.0
     - Create FAB Tables
   * - 92c57b58940d
     - 03afc6b6f902
     - 2.0.0
     - Increase length of FAB ab_view_menu.name column
   * - 03afc6b6f902
     - 849da589634d
     - 2.0.0
     - Prefix DAG permissions.
   * - 849da589634d
     - 2c6edca13270
     - 2.0.0
     - Resource based permissions.
   * - 2c6edca13270
     - 61ec73d9401f
     - 2.0.0
     - Add description field to connection
