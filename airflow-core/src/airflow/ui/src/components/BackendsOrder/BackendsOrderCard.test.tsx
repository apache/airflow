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
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { BackendsOrderCard } from "./BackendsOrderCard";

vi.mock("src/queries/useConfig", () => ({
  useConfig: (key: string) =>
    key === "backends_order" ? "metastore,custom,environment_variable" : undefined,
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    i18n: { dir: () => "ltr" },
    // eslint-disable-next-line id-length
    t: (key: string) => key,
  }),
}));

afterEach(() => {
  cleanup();
});

describe("BackendsOrderCard", () => {
  it("shows the configured secrets backends order when opened", async () => {
    render(<BackendsOrderCard />, { wrapper: BaseWrapper });

    fireEvent.click(screen.getByText("backendsOrder.title"));

    expect(await screen.findByText("metastore,custom,environment_variable")).toBeInTheDocument();
  });
});
