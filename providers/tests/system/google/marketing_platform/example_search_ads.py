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
"""
Example Airflow DAG that shows how to use SearchAds.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.marketing_platform.operators.search_ads import (
    GoogleSearchAdsGetCustomColumnOperator,
    GoogleSearchAdsGetFieldOperator,
    GoogleSearchAdsListCustomColumnsOperator,
    GoogleSearchAdsSearchFieldsOperator,
    GoogleSearchAdsSearchOperator,
)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "search_ads"

# [START howto_search_ads_env_variables]
CUSTOMER_ID: str = os.environ.get("GSA_CUSTOMER_ID", default="")
QUERY = """
    SELECT
        campaign.name,
        campaign.id,
        campaign.status
    FROM campaign;
"""
FIELD_NAME: str = os.environ.get("GSA_FIELD_NAME", default="")
SEARCH_FIELDS_QUERY: str = """
    SELECT
        f1,
        f2
    FROM t1;
"""
CUSTOM_COLUMN_ID: str = os.environ.get("GSA_CUSTOM_COLUMN_ID", default="")
# [END howto_search_ads_env_variables]

with DAG(
    dag_id=DAG_ID,
    schedule="@once",  # Override to match your needs,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["search", "search-ads", "ads"],
) as dag:
    # [START howto_search_ads_search_query_reports]
    query_report = GoogleSearchAdsSearchOperator(
        task_id="query_report",
        customer_id=CUSTOMER_ID,
        query=QUERY,
    )
    # [END howto_search_ads_search_query_reports]

    # [START howto_search_ads_get_field]
    get_field = GoogleSearchAdsGetFieldOperator(
        task_id="get_field",
        field_name=FIELD_NAME,
    )
    # [END howto_search_ads_get_field]

    # [START howto_search_ads_search_fields]
    search_fields = GoogleSearchAdsSearchFieldsOperator(
        task_id="search_fields",
        query=SEARCH_FIELDS_QUERY,
    )
    # [END howto_search_ads_search_fields]

    # [START howto_search_ads_get_custom_column]
    get_custom_column = GoogleSearchAdsGetCustomColumnOperator(
        task_id="get_custom_column",
        customer_id=CUSTOMER_ID,
        custom_column_id=CUSTOM_COLUMN_ID,
    )
    # [END howto_search_ads_get_custom_column]

    # [START howto_search_ads_list_custom_columns]
    list_custom_columns = GoogleSearchAdsListCustomColumnsOperator(
        task_id="list_custom_columns",
        customer_id=CUSTOMER_ID,
    )
    # [END howto_search_ads_list_custom_columns]

    (query_report >> get_field >> search_fields >> get_custom_column >> list_custom_columns)


from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
