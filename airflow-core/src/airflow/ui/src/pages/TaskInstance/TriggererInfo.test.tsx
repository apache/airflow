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
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";

import { Wrapper } from "src/utils/Wrapper";

import { TriggererInfo } from "./TriggererInfo";

describe("TriggererInfo", () => {
  it("links a deferred task's trigger to its page", () => {
    const taskInstance = { trigger: { id: 7 }, triggerer_job: null } as unknown as TaskInstanceResponse;

    render(<TriggererInfo taskInstance={taskInstance} />, { wrapper: Wrapper });

    expect(screen.getByRole("link", { name: "7" })).toHaveAttribute("href", "/triggers/7");
  });
});
