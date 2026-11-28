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
import type { TFunction } from "i18next";
import { describe, expect, it, vi } from "vitest";

import { getColumns } from "./columns";

const translate = ((key: string) => key) as unknown as TFunction;

describe("ActionAccordion columns", () => {
  it("offers no sort, because the rows are held in memory with nothing to reorder them", () => {
    // Every optional column turned on, so none of them can slip past unchecked.
    const columns = getColumns(
      translate,
      { excludedKeys: new Set<string>(), onToggle: vi.fn() },
      { hasLoopIteration: true, hasMultipleDags: true },
    );
    const sortable = columns
      .filter((column) => column.enableSorting !== false)
      .map((column) => ("accessorKey" in column ? column.accessorKey : undefined));

    expect(sortable).toEqual([]);
  });
});
