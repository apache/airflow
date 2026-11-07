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
import { describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import ImportVariablesForm from "./ImportVariablesForm";

const { mutate, setError } = vi.hoisted(() => ({ mutate: vi.fn(), setError: vi.fn() }));

vi.mock("src/queries/useImportVariables", () => ({
  useImportVariables: () => ({ error: undefined, isPending: false, mutate, setError }),
}));

const uploadVariables = (content: string) => {
  const { container } = render(<ImportVariablesForm onClose={vi.fn()} />, { wrapper: Wrapper });
  const input = container.querySelector("input[type=file]");

  if (input === null) {
    throw new Error("File input not found");
  }
  fireEvent.input(input, {
    target: { files: [new File([content], "variables.json", { type: "application/json" })] },
  });
};

describe("ImportVariablesForm", () => {
  it.each(['["example"]', '"example"', "1", "true", "null"])(
    "rejects a non-object JSON document: %s",
    async (content) => {
      uploadVariables(content);

      await waitFor(() =>
        expect(setError).toHaveBeenCalledWith({
          body: { detail: "variables.import.errorParsingJsonFile" },
        }),
      );
      expect(screen.getByRole("button", { name: "variables.import.button" })).toBeDisabled();
      expect(mutate).not.toHaveBeenCalled();
    },
  );

  it("imports a dictionary without changing its variable values", async () => {
    uploadVariables('{"name":"example","config":{"enabled":true}}');
    const button = screen.getByRole("button", { name: "variables.import.button" });

    await waitFor(() => expect(button).toBeEnabled());
    fireEvent.click(button);

    expect(mutate).toHaveBeenCalledWith({
      requestBody: {
        actions: [
          {
            action: "create",
            action_on_existence: "fail",
            entities: [
              { key: "name", value: "example" },
              { key: "config", value: { enabled: true } },
            ],
          },
        ],
      },
    });
  });
});
