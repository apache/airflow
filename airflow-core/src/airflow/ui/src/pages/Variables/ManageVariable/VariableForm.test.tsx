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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import VariableForm, { type VariableBody } from "./VariableForm";

vi.mock("src/queries/useConfig.tsx", () => ({
  useConfig: () => false,
}));

const initialVariable: VariableBody = {
  description: "",
  key: "json_config",
  team_name: "",
  value: '{"enabled": true}',
};

const renderVariableForm = (manageMutate = vi.fn()) => {
  render(
    <VariableForm
      error={undefined}
      initialVariable={initialVariable}
      isPending={false}
      manageMutate={manageMutate}
      setError={vi.fn()}
    />,
    { wrapper: Wrapper },
  );

  return manageMutate;
};

describe("VariableForm", () => {
  it("shows a prominent warning for malformed JSON object values without blocking save", async () => {
    const manageMutate = renderVariableForm();

    fireEvent.change(screen.getByLabelText(/value/iu), { target: { value: '{"enabled": true,' } });

    await waitFor(() => expect(screen.getByText("variables.form.invalidJson")).toBeInTheDocument());
    expect(screen.getByRole("button", { name: /save/iu })).toBeEnabled();

    fireEvent.click(screen.getByRole("button", { name: /save/iu }));

    await waitFor(() =>
      expect(manageMutate).toHaveBeenCalledWith({
        ...initialVariable,
        value: '{"enabled": true,',
      }),
    );
  });

  it("allows saving non-JSON string values that start with a bracket", async () => {
    const manageMutate = renderVariableForm();

    fireEvent.change(screen.getByLabelText(/value/iu), { target: { value: "[DRAFT] plain string value" } });

    await waitFor(() => expect(screen.getByText("variables.form.invalidJson")).toBeInTheDocument());
    expect(screen.getByRole("button", { name: /save/iu })).toBeEnabled();

    fireEvent.click(screen.getByRole("button", { name: /save/iu }));

    await waitFor(() =>
      expect(manageMutate).toHaveBeenCalledWith({
        ...initialVariable,
        value: "[DRAFT] plain string value",
      }),
    );
  });

  it("allows saving Jinja template values", async () => {
    const manageMutate = renderVariableForm();

    fireEvent.change(screen.getByLabelText(/value/iu), { target: { value: "{{ var.value.x }}" } });

    await waitFor(() => expect(screen.getByText("variables.form.invalidJson")).toBeInTheDocument());
    expect(screen.getByRole("button", { name: /save/iu })).toBeEnabled();

    fireEvent.click(screen.getByRole("button", { name: /save/iu }));

    await waitFor(() =>
      expect(manageMutate).toHaveBeenCalledWith({
        ...initialVariable,
        value: "{{ var.value.x }}",
      }),
    );
  });

  it("allows saving plain string values", async () => {
    const manageMutate = renderVariableForm();

    fireEvent.change(screen.getByLabelText(/value/iu), { target: { value: "plain string value" } });

    await waitFor(() => expect(screen.getByRole("button", { name: /save/iu })).toBeEnabled());

    fireEvent.click(screen.getByRole("button", { name: /save/iu }));

    await waitFor(() =>
      expect(manageMutate).toHaveBeenCalledWith({
        ...initialVariable,
        value: "plain string value",
      }),
    );
  });
});
