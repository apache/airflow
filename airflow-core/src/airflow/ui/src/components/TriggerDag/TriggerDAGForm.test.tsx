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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import TriggerDAGForm from "./TriggerDAGForm";

const dagParams = vi.hoisted(() => ({
  paramsDict: {
    message: {
      description: "Message",
      schema: {
        title: "Message",
        type: "string",
      },
      value: "Hello",
    },
  },
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: (translationKey: string) =>
      ({
        "configForm.advancedOptions": "Advanced Options",
      })[translationKey] ?? translationKey,
  }),
}));

vi.mock("src/queries/useDagParams", () => ({
  useDagParams: () => dagParams,
}));

vi.mock("src/queries/useTogglePause", () => ({
  useTogglePause: () => ({
    mutate: vi.fn(),
  }),
}));

vi.mock("../DateTimeInput", () => ({
  DateTimeInput: ({ value = "" }: { readonly value?: string }) => (
    <input aria-label="Logical Date" readOnly value={value} />
  ),
}));

vi.mock("../JsonEditor", () => ({
  JsonEditor: ({
    onBlur,
    onChange,
    value = "",
  }: {
    readonly onBlur?: () => void;
    readonly onChange?: (value: string) => void;
    readonly value?: string;
  }) => (
    <textarea
      aria-label="Configuration JSON"
      onBlur={onBlur}
      onChange={(event) => onChange?.(event.target.value)}
      value={value}
    />
  ),
}));

describe("TriggerDAGForm", () => {
  it("syncs Advanced Options JSON after Run Parameters edits in prefilled re-trigger mode", async () => {
    const { container } = render(
      <TriggerDAGForm
        dagDisplayName="Params Trigger UI"
        dagId="example_params_trigger_ui"
        error={undefined}
        hasSchedule={false}
        isPartitioned={false}
        isPaused={false}
        isPending={false}
        onSubmitTrigger={vi.fn()}
        open
        prefillConfig={{
          conf: {
            message: "Original message",
          },
          logicalDate: undefined,
          runId: "manual__test",
        }}
      />,
      { wrapper: Wrapper },
    );

    await waitFor(() =>
      expect(container.querySelector<HTMLInputElement>('input[name="element_message"]')).toBeInTheDocument(),
    );
    const messageField = container.querySelector<HTMLInputElement>('input[name="element_message"]');

    if (messageField === null) {
      throw new Error("Expected message input to be rendered");
    }

    fireEvent.change(messageField, { target: { value: "Updated message" } });
    fireEvent.click(screen.getByText("Advanced Options"));

    await waitFor(() => {
      const configJson = screen.getByLabelText("Configuration JSON");

      if (!(configJson instanceof HTMLTextAreaElement)) {
        throw new TypeError("Expected Configuration JSON to render as a textarea");
      }

      expect(configJson.value).toContain('"Updated message"');
    });
  });
});
