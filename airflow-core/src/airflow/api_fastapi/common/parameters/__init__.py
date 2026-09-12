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

"""Query parameter helpers for the FastAPI API, re-exported for backward compatibility."""

from __future__ import annotations

from airflow.api_fastapi.common.parameters.asset import (
    QueryAssetAliasNamePatternSearch as QueryAssetAliasNamePatternSearch,
    QueryAssetAliasNamePrefixPatternSearch as QueryAssetAliasNamePrefixPatternSearch,
    QueryAssetDagIdPatternSearch as QueryAssetDagIdPatternSearch,
    QueryAssetDependencyFilter as QueryAssetDependencyFilter,
    QueryAssetEventExtraFilter as QueryAssetEventExtraFilter,
    QueryAssetEventPartitionKeyFilter as QueryAssetEventPartitionKeyFilter,
    QueryAssetEventPartitionKeyRegex as QueryAssetEventPartitionKeyRegex,
    QueryAssetGroupPatternSearch as QueryAssetGroupPatternSearch,
    QueryAssetGroupPrefixPatternSearch as QueryAssetGroupPrefixPatternSearch,
    QueryAssetNamePatternSearch as QueryAssetNamePatternSearch,
    QueryAssetNamePrefixPatternSearch as QueryAssetNamePrefixPatternSearch,
    QueryConsumingAssetPatternSearch as QueryConsumingAssetPatternSearch,
    QueryHasAssetScheduleFilter as QueryHasAssetScheduleFilter,
    QueryPartitionedDagRunDagIdFilter as QueryPartitionedDagRunDagIdFilter,
    QueryPartitionedDagRunHasCreatedDagRunIdFilter as QueryPartitionedDagRunHasCreatedDagRunIdFilter,
    QueryUriExactMatch as QueryUriExactMatch,
    QueryUriPatternSearch as QueryUriPatternSearch,
    QueryUriPrefixPatternSearch as QueryUriPrefixPatternSearch,
)
from airflow.api_fastapi.common.parameters.base import (
    BaseParam as BaseParam,
    LimitFilter as LimitFilter,
    OffsetFilter as OffsetFilter,
    QueryLimit as QueryLimit,
    QueryOffset as QueryOffset,
)
from airflow.api_fastapi.common.parameters.dag import (
    QueryBundleNameFilter as QueryBundleNameFilter,
    QueryBundleVersionFilter as QueryBundleVersionFilter,
    QueryDagDisplayNamePatternSearch as QueryDagDisplayNamePatternSearch,
    QueryDagDisplayNamePrefixPatternSearch as QueryDagDisplayNamePrefixPatternSearch,
    QueryDagIdPatternSearch as QueryDagIdPatternSearch,
    QueryDagIdPatternSearchWithNone as QueryDagIdPatternSearchWithNone,
    QueryDagIdPrefixPatternSearch as QueryDagIdPrefixPatternSearch,
    QueryDagIdPrefixPatternSearchWithNone as QueryDagIdPrefixPatternSearchWithNone,
    QueryDagTagPatternSearch as QueryDagTagPatternSearch,
    QueryDagTagPrefixPatternSearch as QueryDagTagPrefixPatternSearch,
    QueryExcludeStaleFilter as QueryExcludeStaleFilter,
    QueryFavoriteFilter as QueryFavoriteFilter,
    QueryHasImportErrorsFilter as QueryHasImportErrorsFilter,
    QueryOwnersFilter as QueryOwnersFilter,
    QueryPausedFilter as QueryPausedFilter,
    QueryTagsFilter as QueryTagsFilter,
    QueryTeamsFilter as QueryTeamsFilter,
    QueryTimetableTypePrefixPatternSearch as QueryTimetableTypePrefixPatternSearch,
    _DagIdTeamsFilter as _DagIdTeamsFilter,
    teams_filter_factory as teams_filter_factory,
)
from airflow.api_fastapi.common.parameters.dag_run import (
    QueryAnyDagRunStateFilter as QueryAnyDagRunStateFilter,
    QueryDagRunPartitionKeyPrefixSearch as QueryDagRunPartitionKeyPrefixSearch,
    QueryDagRunPartitionKeySearch as QueryDagRunPartitionKeySearch,
    QueryDagRunRunTypesFilter as QueryDagRunRunTypesFilter,
    QueryDagRunStateFilter as QueryDagRunStateFilter,
    QueryDagRunTriggeringUserPrefixSearch as QueryDagRunTriggeringUserPrefixSearch,
    QueryDagRunTriggeringUserSearch as QueryDagRunTriggeringUserSearch,
    QueryDagRunVersionFilter as QueryDagRunVersionFilter,
    QueryLastDagRunStateFilter as QueryLastDagRunStateFilter,
    _transform_dag_run_states as _transform_dag_run_states,
)
from airflow.api_fastapi.common.parameters.filter import (
    FilterOptionEnum as FilterOptionEnum,
    FilterParam as FilterParam,
    filter_param_factory as filter_param_factory,
)
from airflow.api_fastapi.common.parameters.misc import (
    QueryConnectionIdPatternSearch as QueryConnectionIdPatternSearch,
    QueryConnectionIdPrefixPatternSearch as QueryConnectionIdPrefixPatternSearch,
    QueryHITLDetailBodySearch as QueryHITLDetailBodySearch,
    QueryHITLDetailDagIdPatternSearch as QueryHITLDetailDagIdPatternSearch,
    QueryHITLDetailDagIdPrefixPatternSearch as QueryHITLDetailDagIdPrefixPatternSearch,
    QueryHITLDetailMapIndexFilter as QueryHITLDetailMapIndexFilter,
    QueryHITLDetailRespondedUserIdFilter as QueryHITLDetailRespondedUserIdFilter,
    QueryHITLDetailRespondedUserNameFilter as QueryHITLDetailRespondedUserNameFilter,
    QueryHITLDetailResponseReceivedFilter as QueryHITLDetailResponseReceivedFilter,
    QueryHITLDetailSubjectSearch as QueryHITLDetailSubjectSearch,
    QueryHITLDetailTaskIdFilter as QueryHITLDetailTaskIdFilter,
    QueryHITLDetailTaskIdPatternSearch as QueryHITLDetailTaskIdPatternSearch,
    QueryHITLDetailTaskIdPrefixPatternSearch as QueryHITLDetailTaskIdPrefixPatternSearch,
    QueryIncludeDownstream as QueryIncludeDownstream,
    QueryIncludeUpstream as QueryIncludeUpstream,
    QueryParseImportErrorBundleNameFilter as QueryParseImportErrorBundleNameFilter,
    QueryParseImportErrorFilenameFilter as QueryParseImportErrorFilenameFilter,
    QueryParseImportErrorFilenamePatternSearch as QueryParseImportErrorFilenamePatternSearch,
    QueryParseImportErrorFilenamePrefixPatternSearch as QueryParseImportErrorFilenamePrefixPatternSearch,
    QueryPendingActionsFilter as QueryPendingActionsFilter,
    QueryPoolNamePatternSearch as QueryPoolNamePatternSearch,
    QueryPoolNamePrefixPatternSearch as QueryPoolNamePrefixPatternSearch,
    QueryVariableKeyPatternSearch as QueryVariableKeyPatternSearch,
    QueryVariableKeyPrefixPatternSearch as QueryVariableKeyPrefixPatternSearch,
    state_priority as state_priority,
)
from airflow.api_fastapi.common.parameters.range import (
    DateTimeQuery as DateTimeQuery,
    OptionalDateTimeQuery as OptionalDateTimeQuery,
    Range as Range,
    RangeFilter as RangeFilter,
    datetime_range_filter_factory as datetime_range_filter_factory,
    float_range_filter_factory as float_range_filter_factory,
    int_range_filter_factory as int_range_filter_factory,
)
from airflow.api_fastapi.common.parameters.search import (
    _PrefixSearchParam as _PrefixSearchParam,
    _SearchParam as _SearchParam,
    prefix_search_param_factory as prefix_search_param_factory,
    search_param_factory as search_param_factory,
)
from airflow.api_fastapi.common.parameters.sort import (
    SortParam as SortParam,
)
from airflow.api_fastapi.common.parameters.task_instance import (
    QueryTIDagVersionFilter as QueryTIDagVersionFilter,
    QueryTIExecutorFilter as QueryTIExecutorFilter,
    QueryTIMapIndexFilter as QueryTIMapIndexFilter,
    QueryTIOperatorFilter as QueryTIOperatorFilter,
    QueryTIOperatorNamePatternSearch as QueryTIOperatorNamePatternSearch,
    QueryTIOperatorNamePrefixPatternSearch as QueryTIOperatorNamePrefixPatternSearch,
    QueryTIPoolFilter as QueryTIPoolFilter,
    QueryTIPoolNamePatternSearch as QueryTIPoolNamePatternSearch,
    QueryTIPoolNamePrefixPatternSearch as QueryTIPoolNamePrefixPatternSearch,
    QueryTIQueueFilter as QueryTIQueueFilter,
    QueryTIQueueNamePatternSearch as QueryTIQueueNamePatternSearch,
    QueryTIQueueNamePrefixPatternSearch as QueryTIQueueNamePrefixPatternSearch,
    QueryTIRenderedMapIndexPatternSearch as QueryTIRenderedMapIndexPatternSearch,
    QueryTIRenderedMapIndexPrefixPatternSearch as QueryTIRenderedMapIndexPrefixPatternSearch,
    QueryTIStateFilter as QueryTIStateFilter,
    QueryTITaskDisplayNamePatternSearch as QueryTITaskDisplayNamePatternSearch,
    QueryTITaskDisplayNamePrefixPatternSearch as QueryTITaskDisplayNamePrefixPatternSearch,
    QueryTITaskGroupFilter as QueryTITaskGroupFilter,
    QueryTITryNumberFilter as QueryTITryNumberFilter,
)
from airflow.api_fastapi.common.parameters.xcom import (
    QueryXComDagDisplayNamePatternSearch as QueryXComDagDisplayNamePatternSearch,
    QueryXComDagDisplayNamePrefixPatternSearch as QueryXComDagDisplayNamePrefixPatternSearch,
    QueryXComKeyPatternSearch as QueryXComKeyPatternSearch,
    QueryXComKeyPrefixPatternSearch as QueryXComKeyPrefixPatternSearch,
    QueryXComRunIdPatternSearch as QueryXComRunIdPatternSearch,
    QueryXComRunIdPrefixPatternSearch as QueryXComRunIdPrefixPatternSearch,
    QueryXComTaskIdPatternSearch as QueryXComTaskIdPatternSearch,
    QueryXComTaskIdPrefixPatternSearch as QueryXComTaskIdPrefixPatternSearch,
)
