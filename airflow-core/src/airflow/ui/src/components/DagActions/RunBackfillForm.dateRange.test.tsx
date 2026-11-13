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
import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import { describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";

import { Wrapper } from "src/utils/Wrapper";

import RunBackfillForm from "./RunBackfillForm";

dayjs.extend(utc);

const baseDag = vi.hoisted(() => ({
  dag_display_name: "Test Dag",
  dag_id: "test_dag",
  is_paused: false,
  max_active_runs: 10,
  timetable_partitioned: false,
}));

const rangeError = "Start Date must be before the End Date";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    i18n: { language: "en" },
    // eslint-disable-next-line id-length
    t: (key: string) => ({ "backfill.errorStartDateBeforeEndDate": rangeError })[key] ?? key,
  }),
}));

vi.mock("openapi/queries", async (importOriginal) => ({
  ...(await importOriginal<typeof OpenapiQueries>()),
  useBackfillServiceListBackfillsUi: vi.fn(() => ({ data: undefined })),
  useDagRunServiceGetDagRuns: vi.fn(() => ({ data: undefined })),
  useDagServiceGetDagDetails: vi.fn(() => ({ data: baseDag })),
}));

vi.mock("src/queries/useCreateBackfillDryRun", () => ({
  useCreateBackfillDryRun: vi.fn(() => ({ data: undefined, error: undefined, isPending: false })),
}));

vi.mock("src/queries/useCreateBackfill", () => ({
  useCreateBackfill: vi.fn(() => ({
    createBackfill: vi.fn(),
    dateValidationError: undefined,
    error: undefined,
    isPending: false,
    resetError: vi.fn(),
  })),
}));

vi.mock("src/queries/useDagParams", () => ({ useDagParams: vi.fn(() => ({ paramsDict: {} })) }));
vi.mock("src/queries/useParamStore", () => ({ useParamStore: vi.fn(() => ({ conf: "{}" })) }));
vi.mock("src/queries/useTogglePause", () => ({ useTogglePause: vi.fn(() => ({ mutate: vi.fn() })) }));
vi.mock("src/components/Clear/useRerunWithLatestVersion", () => ({
  useRerunWithLatestVersion: vi.fn(() => ({ value: false })),
}));
vi.mock("../ConfigForm", () => ({ default: () => <div data-testid="config-form" /> }));

// Same interaction as `BackfillPage.setBound` in the e2e page objects: open a bound's popover, type a
// date only, then close it so the next bound's inputs are the only ones with these placeholders.
const setBound = async (bound: HTMLElement, date: string) => {
  fireEvent.click(bound);
  fireEvent.change(await screen.findByPlaceholderText("YYYY/MM/DD"), { target: { value: date } });
  fireEvent.click(bound);
  await waitFor(() => expect(screen.queryByPlaceholderText("YYYY/MM/DD")).toBeNull());
};

const renderForm = () => {
  render(<RunBackfillForm dag={baseDag as never} onClose={vi.fn()} />, { wrapper: Wrapper });

  const [fromBound, toBound] = screen.getAllByTestId("datetime-input");

  return { fromBound: fromBound as HTMLElement, toBound: toBound as HTMLElement };
};

describe("RunBackfillForm date range", () => {
  it("shows the range error when From is after To", async () => {
    const { fromBound, toBound } = renderForm();

    await setBound(fromBound, "2025/01/10");
    await setBound(toBound, "2025/01/01");

    expect(await screen.findByText(rangeError)).toBeVisible();
  });

  // Regression for #54429: a date-only entry must yield a valid range.
  it("accepts a date-only range", async () => {
    const { fromBound, toBound } = renderForm();

    await setBound(fromBound, "2025/01/01");
    await setBound(toBound, "2025/01/05");

    await waitFor(() => expect(toBound).toHaveTextContent("Jan 05, 2025"));
    expect(fromBound).toHaveTextContent("Jan 01, 2025");
    expect(screen.queryByText(rangeError)).toBeNull();
  });
});
