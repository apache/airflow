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
import type React from "react";

/**
 * Value carried by a filter. For multi-select filters, this is a string array.
 * `null` is not emitted by the FilterBar; `undefined` means "no value".
 */
export type FilterValue = Date | number | ReadonlyArray<string> | string | null | undefined;

/** Option for a select-style filter. */
export type SelectOption = {
  readonly disabled?: boolean;
  readonly label: string;
  readonly value: string;
};

/** Base properties shared by all filter configs. */
type BaseFilterConfig = {
  readonly defaultValue?: FilterValue;
  /** Disable Cmd/Ctrl+K hotkey when applicable (text filters). */
  readonly hotkeyDisabled?: boolean;
  readonly icon?: React.ReactNode;
  readonly key: string;
  readonly label: string;
  readonly placeholder?: string;
  readonly required?: boolean;
};

/** Discriminated union describing each filter "plugin" configuration. */
export type FilterConfig =
  | ({
      /** Default true. When false, behaves like a radio/single select. */
      readonly multiple?: boolean;
      readonly options: ReadonlyArray<SelectOption>;
      readonly type: "select";
    } & BaseFilterConfig)
  | ({
      readonly max?: number;
      readonly min?: number;
      readonly type: "number";
    } & BaseFilterConfig)
  | ({
      readonly type: "date";
    } & BaseFilterConfig)
  | ({
      readonly type: "text";
    } & BaseFilterConfig);

/** Runtime state per active filter in the bar. */
export type FilterState = {
  readonly config: FilterConfig;
  readonly id: string;
  readonly value: FilterValue;
};

/** Props for the FilterBar host. */
export type FilterBarProps = {
  readonly configs: ReadonlyArray<FilterConfig>;
  readonly initialValues?: Readonly<Record<string, FilterValue>>;
  readonly maxVisibleFilters?: number;
  readonly onFiltersChange: (filters: Record<string, FilterValue>) => void;
};

/** Props injected into individual filter plugins. */
export type FilterPluginProps = {
  readonly filter: FilterState;
  readonly onChange: (value: FilterValue) => void;
  readonly onRemove: () => void;
};
