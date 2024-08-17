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

from airflow.models import BaseOperator
from airflow.providers.magento.hooks.magento import MagentoHook


class GetSalesOrdersOperator(BaseOperator):
    """Fetch orders based on order status from Magento."""

    def __init__(self, magento_conn_id="magento_default", status="pending", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.magento_conn_id = magento_conn_id
        self.status = status

    def execute(self, context):
        magento_hook = MagentoHook(magento_conn_id=self.magento_conn_id)
        search_criteria = {
            "searchCriteria[filterGroups][0][filters][0][field]": "status",
            "searchCriteria[filterGroups][0][filters][0][value]": self.status,
            "searchCriteria[filterGroups][0][filters][0][conditionType]": "eq",
        }
        orders = magento_hook.get_orders(search_criteria=search_criteria)
        if orders and orders.get("total_count", 0) > 0:
            order_items = orders.get("items", [])
            context["ti"].xcom_push(key="new_orders", value=order_items)
        else:
            self.log.info("No new orders found.")
