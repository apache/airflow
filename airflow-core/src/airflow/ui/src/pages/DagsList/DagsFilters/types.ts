/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import type { ComponentProps } from "react";

import type { SearchBar } from "src/components/SearchBar";

export type BooleanFilterValue = "all" | "false" | "true";

type SingleValueFilter = {
  readonly onChange: (value: string | undefined) => void;
  readonly value: string | undefined;
};

type BooleanFilter = {
  readonly onChange: (value: BooleanFilterValue) => void;
  readonly value: BooleanFilterValue;
};

type MultiValueFilter = {
  readonly onChange: (values: Array<string>) => void;
  readonly values: Array<string>;
};

type AsyncSuggestions = {
  readonly hasError?: boolean;
  readonly hasNextPage?: boolean;
  readonly isLoading?: boolean;
  readonly onRetry?: () => void;
};

export type DagsFilterModel = {
  readonly activeRunState: SingleValueFilter;
  readonly clearAll: () => void;
  readonly favorite: BooleanFilter;
  readonly lastRunState: SingleValueFilter;
  readonly multiTeamEnabled: boolean;
  readonly needsReview: BooleanFilter;
  readonly owners: MultiValueFilter;
  readonly paused: BooleanFilter;
  readonly resetSuggestions: () => void;
  readonly tags: AsyncSuggestions &
    MultiValueFilter & {
      readonly matchMode: "all" | "any";
      readonly onInputChange: (value: string) => void;
      readonly onMatchModeChange: (details: { checked: boolean }) => void;
      readonly onMenuScrollToBottom: () => void;
      readonly onMenuScrollToTop: () => void;
      readonly options: Array<string>;
    };
  readonly teams: MultiValueFilter;
  readonly timetableTypes: AsyncSuggestions &
    MultiValueFilter & {
      readonly onInputChange: (value: string) => void;
      readonly onMenuScrollToBottom: () => void;
      readonly onMenuScrollToTop: () => void;
      readonly options: Array<string>;
    };
};

export type DagsFiltersProps = {
  readonly advancedSearch: ComponentProps<typeof SearchBar>["advancedSearch"];
  readonly onSearchChange: (value: string) => void;
  readonly searchValue: string;
};

export type FilterHubProps = {
  readonly model: DagsFilterModel;
} & DagsFiltersProps;
