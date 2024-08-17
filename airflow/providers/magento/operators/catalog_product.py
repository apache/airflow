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
import os

from airflow.models import BaseOperator
from airflow.providers.magento.hooks.magento import MagentoHook


class ProductSyncOperator(BaseOperator):
    """Get product Data from Magento."""

    def __init__(self, output_file="/tmp/magento_products.csv", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output_file = output_file

    def execute(self, context):
        hook = MagentoHook()

        # Get the product data from Magento
        product_data = hook.get_request("products/24-WB02")

        # Ensure the product data is in the format expected (usually a list of dicts)
        if isinstance(product_data, dict):
            product_data = [product_data]

        # Write the product data to a CSV file
        self._write_to_csv(product_data)

        self.log.info("Exported product data to %s", self.output_file)

    def _write_to_csv(self, product_data):
        # Ensure the directory for the output file exists
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)

        # Open the CSV file for writing
        with open(self.output_file, mode="w", newline="") as file:
            if product_data:
                # Extract the headers from the first product's keys
                fieldnames = product_data[0].keys()

                # Create a CSV DictWriter
                writer = csv.DictWriter(file, fieldnames=fieldnames)

                # Write the header
                writer.writeheader()

                # Write the product data
                for product in product_data:
                    writer.writerow(product)
            else:
                self.log.warning("No product data found to write to CSV.")
