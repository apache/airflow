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

import csv

from airflow.models import BaseOperator


class ExportToCSVOperator(BaseOperator):
    """csv export."""

    def __init__(self, file_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_path = file_path

    def execute(self, context):
        ti = context["ti"]
        orders = ti.xcom_pull(task_ids="fetch_orders", key="new_orders")
        if orders and isinstance(orders, list) and orders:
            keys = orders[0].keys()
            with open(self.file_path, "w", newline="") as file:
                dict_writer = csv.DictWriter(file, fieldnames=keys)
                dict_writer.writeheader()
                dict_writer.writerows(orders)
            self.log.info("Data exported to %s", self.file_path)
        else:
            self.log.info("No data to export.")
