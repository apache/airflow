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

export type FilterValue = Date | number | string | null | undefined;

export type FilterConfig = {
  readonly defaultValue?: FilterValue;
  readonly hotkeyDisabled?: boolean;
  readonly icon?: React.ReactNode;
  readonly key: string;
  readonly label: string;
  readonly max?: number;
  readonly min?: number;
  readonly options?: Array<{ label: React.ReactNode | string; value: string }>;
  readonly placeholder?: string;
  readonly required?: boolean;
  readonly type: "date" | "number" | "select" | "text";
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
};

export type FilterPluginProps = {
  readonly filter: FilterState;
  readonly onChange: (value: FilterValue) => void;
  readonly onRemove: () => void;
};
