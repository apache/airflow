# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.operators.hive_to_druid import HiveToDruidTransfer
from airflow import DAG
from datetime import datetime

args = {
            'owner': 'qi_wang',
            'start_date': datetime(2015, 4, 4),
}

dag = DAG("test_druid", default_args=args)


HiveToDruidTransfer(task_id="load_dummy_test",
                    sql="select * from qi.druid_test_dataset_w_platform_1 \
                            limit 10;",
                    druid_datasource="airflow_test",
                    ts_dim="ds",
                    dag=dag
                )
