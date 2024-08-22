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


class GetOrdersOperator(BaseOperator):
    """Fetch orders based on order status from Magento."""

    def __init__(self, magento_conn_id="magento_default", status="pending", page_size=100, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.magento_conn_id = magento_conn_id
        self.status = status
        self.page_size = page_size

    def execute(self, context):
        magento_hook = MagentoHook(magento_conn_id=self.magento_conn_id)
        endpoint = "orders"
        orders = []
        current_page = 1
        while True:
            search_criteria = {
                "searchCriteria[pageSize]": self.page_size,
                "searchCriteria[currentPage]": current_page,
            }

            if self.status:
                search_criteria.update(
                    {
                        "searchCriteria[filterGroups][0][filters][0][field]": "status",
                        "searchCriteria[filterGroups][0][filters][0][value]": self.status,
                        "searchCriteria[filterGroups][0][filters][0][conditionType]": "eq",
                    }
                )
            data = magento_hook.get_request(endpoint, search_criteria=search_criteria)
            if not data["items"]:
                break

            orders.extend(data["items"])
            current_page += 1

        if orders:
            context["ti"].xcom_push(key="magento_orders", value=orders)
        else:
            self.log.info("No new orders found.")
