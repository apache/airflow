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
import { FilterPill } from "../FilterPill";
import type { FilterPluginProps } from "../types";

// A boolean filter is on whenever its pill exists, so the pill is label-only and
// has no editor: picking it from the Add Filter menu turns it on, clicking it turns
// it off. ``renderInput`` is never reached because ``getDefaultFilterValue`` seeds
// the non-empty string "true", leaving ``FilterPill`` out of edit mode.
export const BooleanFilter = ({ filter, onRemove }: FilterPluginProps) => (
  <FilterPill
    displayValue=""
    filter={filter}
    hasValue
    onClick={onRemove}
    onRemove={onRemove}
    renderInput={() => undefined}
  />
);
