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
import type { ReactNode } from "react";

import "@testing-library/jest-dom";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { TFunction } from "i18next";
import { describe, expect, it } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { getInlineMessage } from "./inlineMessage";

const translate = ((key: string, opts?: { count?: number }) => {
  const map: Record<string, string> = {
    "backfill.affected": `${String(opts?.count)} runs will be triggered.`,
    "backfill.affectedNone": "No runs matching selected criteria.",
    "backfill.partitionsAffected": `${String(opts?.count)} partitions will be backfilled:`,
    "backfill.partitionsNone": "No partitions matching selected range.",
  };

  return map[key] ?? key;
}) as TFunction;

const makeBackfills = (partitionKeys: Array<string | null>) =>
  partitionKeys.map((partitionKey) => ({
    logical_date: null,
    partition_date: null,
    partition_key: partitionKey,
  }));

const renderMessage = (el: ReactNode) => render(el, { wrapper: Wrapper });

const makeHourlyKeys = (count: number) =>
  Array.from({ length: count }, (_, idx) => `2024-01-${String(idx + 1).padStart(2, "0")}T00`);

// The DataTable re-slices data asynchronously after a page change, so `table-cell-partition_key`
// matches non-empty before and after the click -- `findBy*` would resolve on the stale render.
// Poll until the row count settles on the expected page instead.
const assertPartitionKeyCellCount = (length: number) =>
  waitFor(() => expect(screen.getAllByTestId("table-cell-partition_key")).toHaveLength(length));

describe("getInlineMessage", () => {
  describe("non-partitioned", () => {
    it("shows skeleton while pending", () => {
      const el = getInlineMessage({ isPendingDryRun: true, totalEntries: 0, translate });
      const { container } = renderMessage(el);

      expect(container.querySelector(".chakra-skeleton")).toBeInTheDocument();
    });

    it("shows error text when totalEntries is 0", () => {
      const el = getInlineMessage({ isPendingDryRun: false, totalEntries: 0, translate });

      renderMessage(el);
      expect(screen.getByText("No runs matching selected criteria.")).toBeInTheDocument();
    });

    it("shows count message when totalEntries > 0", () => {
      const el = getInlineMessage({ isPendingDryRun: false, totalEntries: 3, translate });

      renderMessage(el);
      expect(screen.getByText("3 runs will be triggered.")).toBeInTheDocument();
    });
  });

  describe("partitioned", () => {
    it("shows 'no partitions' message when totalEntries is 0", () => {
      const el = getInlineMessage({
        isPartitioned: true,
        isPendingDryRun: false,
        totalEntries: 0,
        translate,
      });

      renderMessage(el);
      expect(screen.getByText("No partitions matching selected range.")).toBeInTheDocument();
    });

    it("lists partition_key values when totalEntries > 0", () => {
      const backfills = makeBackfills(["2024-01-01T00/2024-01-01T01", "2024-01-01T01/2024-01-01T02"]);
      const el = getInlineMessage({
        backfills,
        isPartitioned: true,
        isPendingDryRun: false,
        totalEntries: 2,
        translate,
      });

      renderMessage(el);
      expect(screen.getByText("2 partitions will be backfilled:")).toBeInTheDocument();
      expect(screen.getByText("2024-01-01T00/2024-01-01T01")).toBeInTheDocument();
      expect(screen.getByText("2024-01-01T01/2024-01-01T02")).toBeInTheDocument();
      // Partition keys render one per row in a table rather than joined inline.
      expect(screen.getAllByTestId("table-cell-partition_key")).toHaveLength(2);
      expect(screen.queryByTestId("next")).not.toBeInTheDocument();
    });

    it("skips null partition_key entries", () => {
      const backfills = makeBackfills([null, "2024-01-01T00/2024-01-01T01"]);
      const el = getInlineMessage({
        backfills,
        isPartitioned: true,
        isPendingDryRun: false,
        totalEntries: 2,
        translate,
      });

      renderMessage(el);
      expect(screen.queryByText("null")).not.toBeInTheDocument();
      expect(screen.getByText("2024-01-01T00/2024-01-01T01")).toBeInTheDocument();
      expect(screen.getAllByTestId("table-cell-partition_key")).toHaveLength(1);
      expect(screen.queryByTestId("next")).not.toBeInTheDocument();
    });

    it("shows pagination controls when entries exceed the page size", () => {
      const keys = makeHourlyKeys(12);
      const backfills = makeBackfills(keys);
      const el = getInlineMessage({
        backfills,
        isPartitioned: true,
        isPendingDryRun: false,
        totalEntries: 12,
        translate,
      });

      renderMessage(el);
      expect(screen.getByText("12 partitions will be backfilled:")).toBeInTheDocument();
      // Only the first page (page size 10) renders; the rest are reachable via the next page.
      expect(screen.getAllByTestId("table-cell-partition_key")).toHaveLength(10);
      expect(screen.getByTestId("next")).toBeInTheDocument();
    });

    it("shows pagination controls when entries are exactly one over the page size", async () => {
      // 11 keys: 10 shown on the first page, 1 reachable via the next page. Even a single
      // remaining key must trigger pagination -- unlike the old popover affordance, which
      // rendered a lone remainder inline without one.
      const keys = makeHourlyKeys(11);
      const backfills = makeBackfills(keys);
      const el = getInlineMessage({
        backfills,
        isPartitioned: true,
        isPendingDryRun: false,
        totalEntries: 11,
        translate,
      });

      renderMessage(el);
      expect(screen.getAllByTestId("table-cell-partition_key")).toHaveLength(10);
      expect(screen.getByTestId("next")).toBeInTheDocument();

      const remainingKeys = keys.slice(10);

      fireEvent.click(screen.getByTestId("next"));
      await assertPartitionKeyCellCount(remainingKeys.length);
      for (const key of remainingKeys) {
        expect(screen.getByText(key)).toBeInTheDocument();
      }
    });

    it("does not show pagination controls when entries are within the page size", () => {
      const keys = makeHourlyKeys(5);
      const backfills = makeBackfills(keys);
      const el = getInlineMessage({
        backfills,
        isPartitioned: true,
        isPendingDryRun: false,
        totalEntries: 5,
        translate,
      });

      renderMessage(el);
      expect(screen.queryByTestId("next")).not.toBeInTheDocument();
      for (const key of keys) {
        expect(screen.getByText(key)).toBeInTheDocument();
      }
    });

    it("does not show pagination controls when entries exactly equal the page size", () => {
      // Mirrors the pageSize (10) constant used by PartitionPreviewTable.
      const pageSize = 10;
      const keys = makeHourlyKeys(pageSize);
      const backfills = makeBackfills(keys);
      const el = getInlineMessage({
        backfills,
        isPartitioned: true,
        isPendingDryRun: false,
        totalEntries: pageSize,
        translate,
      });

      renderMessage(el);

      for (const key of keys) {
        expect(screen.getByText(key)).toBeInTheDocument();
      }
      expect(screen.queryByTestId("next")).not.toBeInTheDocument();
    });

    it("bases pagination on non-null keys, not totalEntries, when nulls are mixed with overflow", async () => {
      const nullEntries: Array<string | null> = [null, null, null];
      const keys = makeHourlyKeys(12);
      const backfills = makeBackfills([...nullEntries, ...keys]);
      const el = getInlineMessage({
        backfills,
        isPartitioned: true,
        isPendingDryRun: false,
        totalEntries: 15,
        translate,
      });

      renderMessage(el);
      expect(screen.getByText("15 partitions will be backfilled:")).toBeInTheDocument();
      // 12 non-null keys, page size 10 -> next page holds 2, regardless of totalEntries (15).
      expect(screen.getByTestId("next")).toBeInTheDocument();
      fireEvent.click(screen.getByTestId("next"));
      await assertPartitionKeyCellCount(2);
    });
  });
});
