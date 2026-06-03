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
import { describe, expect, it } from "vitest";

import type { PoolResponse } from "openapi/requests/types.gen";
import { Wrapper } from "src/utils/Wrapper";

import { PoolBar } from "./PoolBar";

const createPool = (pool: Partial<PoolResponse>): PoolResponse => ({
  deferred_slots: 1,
  description: null,
  include_deferred: false,
  name: "default_pool",
  occupied_slots: 1,
  open_slots: 128,
  queued_slots: 0,
  running_slots: 0,
  scheduled_slots: 0,
  slots: 128,
  team_name: null,
  ...pool,
});

describe("PoolBar", () => {
  it("shows deferred slots as secondary info when they do not consume pool slots", () => {
    const { container } = render(
      <PoolBar pool={createPool({ include_deferred: false })} totalSlots={128} />,
      {
        wrapper: Wrapper,
      },
    );

    expect(container.querySelector('a[href*="task_state=deferred"]')).not.toBeInTheDocument();
    expect(screen.getByText(/common:states\.deferred/u)).toHaveTextContent("1");
  });

  it("shows deferred slots in the usage bar when they consume pool slots", () => {
    const { container } = render(
      <PoolBar pool={createPool({ include_deferred: true, open_slots: 127 })} totalSlots={128} />,
      { wrapper: Wrapper },
    );

    expect(container.querySelector('a[href*="task_state=deferred"]')).toBeInTheDocument();
    expect(screen.getByText("1")).toBeInTheDocument();
  });
});
