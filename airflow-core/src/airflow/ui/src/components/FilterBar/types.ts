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

// Value of a composite filter (one whose config sets ``fromSearchParams``/``toSearchParams``).
// Its concrete shape is private to that filter's editor and projections; the bar treats it opaquely.
export type CompositeFilterValue = Record<string, string>;

// A ``boolean`` filter's value is the string ``"true"``; absent means off. Keeping it a
// string avoids ``isValidFilterValue`` treating ``false`` as a value worth writing to the URL.
export type FilterValue =
  Array<string> | CompositeFilterValue | Date | DateRangeValue | number | string | null | undefined;

export type FilterConfig = {
  readonly defaultValue?: FilterValue;
  // Bespoke editor for filters a static ``options`` array cannot express, such as
  // search-as-you-type over a paginated endpoint. Must be a component rather than a
  // render callback so the editor can own its own hooks.
  readonly EditorComponent?: ComponentType<FilterPluginProps>;
  readonly endKey?: string;
  // Composite filters span several URL params (e.g. one pill writing a state param plus a time
  // bound). ``fromSearchParams`` reads the pill's value from the URL, undefined meaning no filter;
  // ``toSearchParams`` projects a value onto the params the filter manages and must enumerate
  // every one of them (mapped to undefined to clear), so removing the pill wipes them all.
  // Filters without these hooks read and write ``key`` directly.
  readonly fromSearchParams?: (params: URLSearchParams) => FilterValue;
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
  readonly options?: Array<{ label: ReactNode | string; value: string }>;
  readonly placeholder?: string;
  readonly startKey?: string;
  // Set on text filters whose API endpoint exposes both ``*_pattern`` (substring)
  // and ``*_prefix_pattern`` (prefix) variants. The pill renders a toggle that
  // controls which one the consuming page uses, via ``useAdvancedSearch``.
  readonly supportsAdvancedSearch?: boolean;
  readonly toSearchParams?: (value: FilterValue) => Record<string, string | undefined>;
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
