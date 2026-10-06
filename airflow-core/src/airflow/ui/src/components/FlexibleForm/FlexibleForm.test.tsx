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
import { describe, expect, it, vi } from "vitest";

import type { ParamsSpec } from "src/queries/useDagParams";
import { Wrapper } from "src/utils/Wrapper";

import { FlexibleForm } from "./FlexibleForm";

const intervalParams = (start: string, end: string) =>
  ({
    end: {
      description: null,
      schema: { format: "date", formatExclusiveMinimum: { $data: "1/start" }, type: "string" },
      value: end,
    },
    start: { description: null, schema: { format: "date", type: "string" }, value: start },
  }) as unknown as ParamsSpec;

describe("FlexibleForm — params bounded by another param", () => {
  it("flags the form and shows the error under the field when the end is not after the start", async () => {
    const setError = vi.fn();

    render(
      <FlexibleForm
        flexibleFormDefaultSection="Params"
        initialParamsDict={{ paramsDict: intervalParams("2026-10-02", "2026-10-01") }}
        namespace="bounded-invalid"
        noAccordion
        setError={setError}
      />,
      { wrapper: Wrapper },
    );

    expect(await screen.findByText("flexibleForm.validationErrorAfter")).toBeInTheDocument();
    expect(setError).toHaveBeenLastCalledWith(true);
  });
});
