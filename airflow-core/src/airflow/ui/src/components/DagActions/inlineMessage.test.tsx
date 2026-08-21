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
import "@testing-library/jest-dom";
import { render, screen } from "@testing-library/react";
import type { TFunction } from "i18next";
import type { ReactNode } from "react";
import { describe, expect, it } from "vitest";

import i18n from "src/i18n/config";
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

// LimitedItemsList uses its own useTranslation("components") hook rather than the
// injected `translate` stub above, so its "+N more" affordance text is derived the
// same way the component derives it: via the real (module-singleton) i18n instance.
const moreButtonName = (count: number) => i18n.t("limitedList", { count, ns: "components" });

const makeBackfills = (partitionKeys: Array<string | null>) =>
  partitionKeys.map((partitionKey) => ({
    logical_date: null,
    partition_date: null,
    partition_key: partitionKey,
  }));

const renderMessage = (el: ReactNode) => render(el, { wrapper: Wrapper });

const makeHourlyKeys = (count: number) =>
  Array.from({ length: count }, (_, idx) => `2024-01-${String(idx + 1).padStart(2, "0")}T00`);

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
      // Partition keys render one per line rather than joined inline, since keys can be long.
      expect(screen.getByTestId("limited-items-vertical")).toBeInTheDocument();
      expect(screen.getAllByTestId("limited-items-item")).toHaveLength(2);
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
      expect(screen.queryByTestId("limited-items-expand-button")).not.toBeInTheDocument();
    });

    it("shows an expandable 'N more' summary when entries exceed the preview limit", () => {
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
      expect(screen.getByRole("button", { name: moreButtonName(2) })).toBeInTheDocument();
      // Only the first 10 (maxItems) keys render as by-line items; the rest stay behind the popover.
      expect(screen.getAllByTestId("limited-items-item")).toHaveLength(10);
    });

    it("counts the single remaining key as a by-line item when exactly one is over the limit", () => {
      // 11 keys: 10 displayed + 1 remaining. With exactly 1 remaining, LimitedItemsList
      // renders it directly (no popover) — it must still count as a by-line item since it
      // renders on its own line in the vertical list, same as the first 10.
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
      expect(screen.queryByTestId("limited-items-expand-button")).not.toBeInTheDocument();
      expect(screen.getAllByTestId("limited-items-item")).toHaveLength(11);
      for (const key of keys) {
        expect(screen.getByText(key)).toBeInTheDocument();
      }
    });

    it("does not show an expand affordance when entries are within the preview limit", () => {
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
      expect(screen.queryByTestId("limited-items-expand-button")).not.toBeInTheDocument();
    });

    it("does not show an expand affordance when entries exactly equal the preview limit", () => {
      // Mirrors the PARTITION_PREVIEW_LIMIT (10) defined in inlineMessage.tsx.
      const previewLimit = 10;
      const keys = makeHourlyKeys(previewLimit);
      const backfills = makeBackfills(keys);
      const el = getInlineMessage({
        backfills,
        isPartitioned: true,
        isPendingDryRun: false,
        totalEntries: previewLimit,
        translate,
      });

      renderMessage(el);

      for (const key of keys) {
        expect(screen.getByText(key)).toBeInTheDocument();
      }
      expect(screen.queryByTestId("limited-items-expand-button")).not.toBeInTheDocument();
    });

    it("bases the remaining count on non-null keys, not totalEntries, when nulls are mixed with overflow", () => {
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
      // 12 non-null keys, preview limit 10 -> 2 remaining, regardless of totalEntries (15).
      expect(screen.getByRole("button", { name: moreButtonName(2) })).toBeInTheDocument();
    });
  });
});
