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
import { describe, expect, it } from "vitest";

import "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import { TriggerStatusBadge } from "./TriggerStatusBadge";

// Tests render i18n keys: the msw handlers serve empty translation bundles.
describe("TriggerStatusBadge", () => {
  it.each(["orphaned", "failing", "pending", "running"] as const)(
    "explains what %s means on hover",
    async (status) => {
      render(<TriggerStatusBadge status={status} />, { wrapper: Wrapper });

      fireEvent.pointerEnter(screen.getByTestId("trigger-status-badge"), { pointerType: "mouse" });

      expect(await screen.findByText(`triggers.statusHint.${status}`)).toBeInTheDocument();
    },
  );
});
