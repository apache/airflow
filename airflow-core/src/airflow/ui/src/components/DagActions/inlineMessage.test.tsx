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

import { Wrapper } from "src/utils/Wrapper";

import { getInlineMessage } from "./inlineMessage";

const translate = ((key: string, opts?: { count?: number }) => {
  const map: Record<string, string> = {
    "backfill.affected": `${String(opts?.count)} runs will be triggered.`,
    "backfill.affectedNone": "No runs matching selected criteria.",
    "backfill.andOthers": `…and ${String(opts?.count)} more`,
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
      expect(screen.queryByText(/…and|and .* more/iu)).not.toBeInTheDocument();
    });

    it("shows 'and N more' summary when entries exceed preview limit", () => {
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
      expect(screen.getByText("…and 2 more")).toBeInTheDocument();
    });

    it("does not show 'and N more' when entries are within preview limit", () => {
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
      expect(screen.queryByText(/…and/u)).not.toBeInTheDocument();
    });
  });
});
