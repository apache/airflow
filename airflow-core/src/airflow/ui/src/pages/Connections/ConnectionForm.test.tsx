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
import { beforeEach, describe, expect, it, vi } from "vitest";

import i18n from "src/i18n/config";
import type { ConnectionMetaEntry } from "src/queries/useConnectionTypeMeta";
import { paramPlaceholder } from "src/queries/useParamStore";
import { Wrapper } from "src/utils/Wrapper";

import adminLocale from "../../../public/i18n/locales/en/admin.json";
import ConnectionForm from "./ConnectionForm";
import type { ConnectionBody } from "./Connections";

const { mockUseConnectionTypeMeta } = vi.hoisted(() => ({
  mockUseConnectionTypeMeta: vi.fn(),
}));

vi.mock("src/queries/useConnectionTypeMeta", () => ({
  useConnectionTypeMeta: mockUseConnectionTypeMeta,
}));

vi.mock("src/queries/useConfig.tsx", () => ({
  useConfig: () => false,
}));

const pydanticAiMeta: ConnectionMetaEntry = {
  connection_type: "pydanticai",
  default_conn_name: "pydanticai_default",
  extra_fields: {
    model: {
      description: "Model in provider:name format",
      schema: { ...paramPlaceholder.schema, title: "Model", type: ["string", "null"] },
      value: null,
    },
  },
  hook_class_name: "airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook",
  hook_name: "Pydantic AI",
  standard_fields: {},
};

const loadedMeta = {
  formattedData: { pydanticai: pydanticAiMeta },
  hookNames: { pydanticai: "Pydantic AI" },
  isPending: false,
  keysList: ["pydanticai"],
};

const pendingMeta = { formattedData: {}, hookNames: {}, isPending: true, keysList: [] };

const pydanticAiConnection: ConnectionBody = {
  conn_type: "pydanticai",
  connection_id: "decision_default",
  description: "",
  extra: JSON.stringify({ model: "openai:gpt-5" }),
  host: "",
  login: "",
  password: "",
  port: "",
  schema: "",
  team_name: "",
};

const renderForm = () => (
  <ConnectionForm
    error={undefined}
    initialConnection={pydanticAiConnection}
    isEditMode
    isPending={false}
    mutateConnection={vi.fn()}
  />
);

describe("ConnectionForm", () => {
  beforeEach(() => {
    i18n.addResourceBundle("en", "admin", adminLocale, true, true);
  });

  it("titles the provider fields section after the connection type and opens it by default", () => {
    mockUseConnectionTypeMeta.mockReturnValue(loadedMeta);

    render(renderForm(), { wrapper: Wrapper });

    expect(screen.getByRole("button", { name: "Pydantic AI Fields" })).toHaveAttribute(
      "aria-expanded",
      "true",
    );
    expect(screen.getByRole("button", { name: "Standard Fields" })).toHaveAttribute("aria-expanded", "true");
  });

  it("keeps the saved extra and opens the section when hook metadata loads after the form mounts", () => {
    mockUseConnectionTypeMeta.mockReturnValue(pendingMeta);
    const { rerender } = render(renderForm(), { wrapper: Wrapper });

    mockUseConnectionTypeMeta.mockReturnValue(loadedMeta);
    rerender(renderForm());

    expect(screen.getByRole("button", { name: "Pydantic AI Fields" })).toHaveAttribute(
      "aria-expanded",
      "true",
    );
    expect(document.querySelector<HTMLInputElement>("#element_model")).toHaveValue("openai:gpt-5");
  });
});
