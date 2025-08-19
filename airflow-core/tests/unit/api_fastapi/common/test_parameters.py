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

import re
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException
from sqlalchemy.sql import Select

from airflow.api_fastapi.common.parameters import SortParam, _IsDagScheduledFilter


class TestSortParam:
    def test_sort_param_max_number_of_filers(self):
        param = SortParam([], None, None)
        n_filters = param.MAX_SORT_PARAMS + 1
        param.value = [f"filter_{i}" for i in range(n_filters)]

        with pytest.raises(
            HTTPException,
            match=re.escape(
                f"400: Ordering with more than {param.MAX_SORT_PARAMS} parameters is not allowed. Provided: {param.value}"
            ),
        ):
            param.to_orm(None)


class TestIsDagScheduledFilter:
    @pytest.fixture
    def mock_dagmodel(self):
        """
        Patch DagModel in the parameters module's namespace so the filter uses our mock.
        """
        with patch("airflow.api_fastapi.common.parameters.DagModel", autospec=True) as mock_dm:
            timetable_mock = MagicMock()
            mock_dm.timetable_description = timetable_mock
            yield mock_dm

    @pytest.fixture
    def mock_select(self):
        """
        Create a mock SQLAlchemy Select with .where().
        """
        select = MagicMock(spec=Select)
        select.where.return_value = "filtered"
        return select

    def test_to_orm_value_none_returns_original(self, mock_select):
        f = _IsDagScheduledFilter()
        f.value = None
        f.skip_none = True

        result = f.to_orm(mock_select)

        assert result == mock_select
        mock_select.where.assert_not_called()

    def test_to_orm_false_applies_never_filter(self, mock_select, mock_dagmodel):
        f = _IsDagScheduledFilter()
        f.value = False
        f.skip_none = True

        mock_condition = MagicMock(name="ilike_condition")
        mock_dagmodel.timetable_description.ilike.return_value = mock_condition

        result = f.to_orm(mock_select)

        assert result == "filtered"
        mock_dagmodel.timetable_description.ilike.assert_called_once_with("Never%")
        mock_select.where.assert_called_once_with(mock_condition)

    def test_to_orm_true_applies_not_never_filter(self, mock_select, mock_dagmodel):
        f = _IsDagScheduledFilter()
        f.value = True
        f.skip_none = True

        mock_condition = MagicMock(name="not_like_condition")
        mock_dagmodel.timetable_description.not_like.return_value = mock_condition

        result = f.to_orm(mock_select)

        assert result == "filtered"
        mock_dagmodel.timetable_description.not_like.assert_called_once_with("Never%")
        mock_select.where.assert_called_once_with(mock_condition)

    def test_depends_sets_value(self):
        f = _IsDagScheduledFilter.depends(True)
        assert isinstance(f, _IsDagScheduledFilter)
        assert f.value is True
