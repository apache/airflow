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
"""System tests for Google BigQuery hooks"""

from __future__ import annotations

import pytest

from airflow.providers.google.cloud.hooks import bigquery as hook

from tests_common.test_utils.gcp_system_helpers import GoogleSystemTest
from unit.google.cloud.utils.gcp_authenticator import GCP_BIGQUERY_KEY


@pytest.mark.system
@pytest.mark.credential_file(GCP_BIGQUERY_KEY)
class TestBigQueryDataframeResultsSystem(GoogleSystemTest):
    def setup_method(self):
        self.instance = hook.BigQueryHook()

    @pytest.mark.parametrize("df_type", ["pandas", "polars"])
    def test_output_is_dataframe_with_valid_query(self, df_type):
        df = self.instance.get_df("select 1", df_type=df_type)
        if df_type == "polars":
            import polars as pl

            assert isinstance(df, pl.DataFrame)
        else:
            import pandas as pd

            assert isinstance(df, pd.DataFrame)

    @pytest.mark.parametrize("df_type", ["pandas", "polars"])
    def test_throws_exception_with_invalid_query(self, df_type):
        with pytest.raises(Exception) as ctx:
            self.instance.get_df("from `1`", df_type=df_type)
        assert "Reason: " in str(ctx.value), ""

    @pytest.mark.parametrize("df_type", ["pandas", "polars"])
    def test_succeeds_with_explicit_legacy_query(self, df_type):
        df = self.instance.get_df("select 1", df_type=df_type)
        if df_type == "polars":
            assert df.item(0, 0) == 1
        else:
            assert df.iloc[0][0] == 1

    @pytest.mark.parametrize("df_type", ["pandas", "polars"])
    def test_succeeds_with_explicit_std_query(self, df_type):
        df = self.instance.get_df(
            "select * except(b) from (select 1 a, 2 b)",
            parameters=None,
            dialect="standard",
            df_type=df_type,
        )
        if df_type == "polars":
            assert df.item(0, 0) == 1
        else:
            assert df.iloc[0][0] == 1

    @pytest.mark.parametrize("df_type", ["pandas", "polars"])
    def test_throws_exception_with_incompatible_syntax(self, df_type):
        with pytest.raises(Exception) as ctx:
            self.instance.get_df(
                "select * except(b) from (select 1 a, 2 b)",
                parameters=None,
                dialect="legacy",
                df_type=df_type,
            )
        assert "Reason: " in str(ctx.value), ""
