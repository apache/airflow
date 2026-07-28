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
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { HITLDetail } from "openapi/requests/types.gen.ts";
import { Wrapper } from "src/utils/Wrapper";

import { HITLReviewModal } from "./HITLReviewModal";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: (key: string) => key,
  }),
}));

vi.mock("src/components/HITLReview/HITLReviewDetail.tsx", () => ({
  HITLReviewDetail: () => null,
}));

const mockHitl = {
  created_at: "2024-01-01T00:00:00Z",
  options: ["Approve", "Reject"],
  subject: "Test subject",
  task_instance: {
    dag_id: "test_dag",
    dag_run_id: "test_run",
    id: "pending",
    map_index: -1,
    rendered_map_index: null,
    run_after: "2024-01-01T00:00:00Z",
    state: "awaiting_input",
    task_id: "task-pending",
    try_number: 1,
  },
} as HITLDetail;

const completedHitl = {
  ...mockHitl,
  task_instance: {
    ...mockHitl.task_instance,
    id: "completed",
    task_id: "task-completed",
  },
};

const renderModal = ({ withCompleted = false }: { readonly withCompleted?: boolean } = {}) =>
  render(
    <HITLReviewModal
      completedHitl={withCompleted ? { data: [completedHitl] } : undefined}
      onClose={vi.fn()}
      open
      pendingHitl={{ data: [mockHitl] }}
    />,
    { wrapper: Wrapper },
  );

describe("HITLReviewModal", () => {
  it("renders the required actions dialog", () => {
    renderModal();

    expect(screen.getByRole("dialog", { name: "requiredAction_other" })).toBeInTheDocument();
  });

  it("does not render the completed filter when completed HITL data is not provided", () => {
    renderModal();

    expect(screen.queryByRole("button", { name: "filters.response.all" })).toBeNull();
  });

  it("renders the completed filter when completed HITL data is provided", () => {
    renderModal({ withCompleted: true });

    expect(screen.getByRole("button", { name: "filters.response.all" })).toBeInTheDocument();
  });

  it("shows only pending HITL rows by default", () => {
    renderModal({ withCompleted: true });

    expect(screen.getByText("task-pending")).toBeInTheDocument();
    expect(screen.queryByText("task-completed")).toBeNull();
  });

  it("shows completed HITL rows when switching to all required actions", () => {
    renderModal({ withCompleted: true });

    fireEvent.click(screen.getByRole("button", { name: "filters.response.all" }));

    expect(screen.getByText("task-completed")).toBeInTheDocument();
  });
});
