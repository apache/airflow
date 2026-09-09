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
import type { ComponentType, ReactNode } from "react";

export type DateRangeValue = {
  endDate?: string;
  startDate?: string;
};

// A ``boolean`` filter's value is the string ``"true"``; absent means off. Keeping it a
// string avoids ``isValidFilterValue`` treating ``false`` as a value worth writing to the URL.
export type FilterValue = Array<string> | Date | DateRangeValue | number | string | null | undefined;

export type FilterConfig = {
  readonly defaultValue?: FilterValue;
  // Bespoke editor for filters a static ``options`` array cannot express, such as
  // search-as-you-type over a paginated endpoint. Must be a component rather than a
  // render callback so the editor can own its own hooks.
  readonly EditorComponent?: ComponentType<FilterPluginProps>;
  readonly endKey?: string;
  readonly hotkeyDisabled?: boolean;
  readonly icon?: ReactNode;
  // Multiselect only: accept free-text values that are not in ``options``.
  readonly isCreatable?: boolean;
  readonly key: string;
  readonly label: string;
  // Multiselect only: URL param holding this filter's any/all match mode. Cleared
  // alongside the filter, which ``useFiltersHandler`` would not otherwise manage.
  readonly matchModeKey?: string;
  readonly max?: number;
  readonly min?: number;
  // ``label`` is the plain-text form shown in the pill and the select trigger; it must
  // stay a string because both are fixed-height, and a rich node there gets clipped.
  // ``menuItem`` optionally restyles the option inside the dropdown menu only, e.g. a
  // ``StateBadge`` for run states.
  readonly options?: Array<{ label: string; menuItem?: ReactNode; value: string }>;
  readonly placeholder?: string;
  readonly startKey?: string;
  // Set on text filters whose API endpoint exposes both ``*_pattern`` (substring)
  // and ``*_prefix_pattern`` (prefix) variants. The pill renders a toggle that
  // controls which one the consuming page uses, via ``useAdvancedSearch``.
  readonly supportsAdvancedSearch?: boolean;
  readonly type: "boolean" | "date" | "daterange" | "multiselect" | "number" | "select" | "text";
};

export type FilterState = {
  readonly config: FilterConfig;
  readonly id: string;
  readonly value: FilterValue;
};

export type FilterBarProps = {
  readonly configs: Array<FilterConfig>;
  readonly initialValues?: Record<string, FilterValue>;
  readonly maxVisibleFilters?: number;
  readonly onFiltersChange: (filters: Record<string, FilterValue>) => void;
  // Hide the Preset Filters control where they aren't useful (e.g. a Dag run's per-run task instances).
  readonly showPresetFilters?: boolean;
};

export type FilterPluginProps = {
  readonly filter: FilterState;
  readonly onChange: (value: FilterValue) => void;
  readonly onRemove: () => void;
};
