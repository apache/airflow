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
import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { ParamsSpec } from "src/queries/useDagParams";
import { Wrapper } from "src/utils/Wrapper";

import ConnectionForm from "./ConnectionForm";
import type { ConnectionBody } from "./Connections";

const { snowflakeExtraFields } = vi.hoisted(() => ({
  snowflakeExtraFields: {
    account: {
      description: null,
      schema: {
        const: undefined,
        description_md: undefined,
        enum: undefined,
        examples: undefined,
        format: undefined,
        items: undefined,
        maximum: undefined,
        maxLength: undefined,
        minimum: undefined,
        minLength: undefined,
        section: undefined,
        title: "Account",
        type: ["string", "null"],
        values_display: undefined,
      },
      value: undefined,
    },
    insecure_mode: {
      description: "Turns off OCSP certificate checks",
      schema: {
        const: undefined,
        description_md: undefined,
        enum: undefined,
        examples: undefined,
        format: undefined,
        items: undefined,
        maximum: undefined,
        maxLength: undefined,
        minimum: undefined,
        minLength: undefined,
        section: undefined,
        title: "Insecure Mode",
        type: ["boolean", "null"],
        values_display: undefined,
      },
      value: false,
    },
  } satisfies ParamsSpec,
}));

vi.mock("src/components/JsonEditor", () => ({
  JsonEditor: ({
    onBlur,
    onChange,
    value,
  }: {
    readonly onBlur?: () => void;
    readonly onChange?: (value: string) => void;
    readonly value?: string;
  }) => (
    <textarea
      aria-label="extra-json"
      onBlur={onBlur}
      onChange={(event) => onChange?.(event.target.value)}
      value={value ?? ""}
    />
  ),
}));

vi.mock("src/queries/useConfig.tsx", () => ({
  useConfig: () => false,
}));

vi.mock("src/queries/useConnectionTypeMeta", () => ({
  useConnectionTypeMeta: () => ({
    formattedData: {
      snowflake: {
        connection_type: "snowflake",
        default_conn_name: undefined,
        extra_fields: snowflakeExtraFields,
        hook_class_name: "airflow.providers.snowflake.hooks.snowflake.SnowflakeHook",
        hook_name: "Snowflake",
        standard_fields: {},
      },
    },
    hookNames: { snowflake: "Snowflake" },
    isPending: false,
    keysList: ["snowflake"],
  }),
}));

const initialConnection: ConnectionBody = {
  conn_type: "snowflake",
  connection_id: "snowflake_default",
  description: "",
  extra: '{"account":"1234"}',
  host: "",
  login: "",
  password: "",
  port: "",
  schema: "",
  team_name: "",
};

describe("ConnectionForm", () => {
  it("does not rewrite raw Snowflake extras with untouched provider defaults", async () => {
    render(
      <ConnectionForm
        error={undefined}
        initialConnection={initialConnection}
        isEditMode
        isPending={false}
        mutateConnection={vi.fn()}
      />,
      { wrapper: Wrapper },
    );

    const extraJson = await screen.findByLabelText("extra-json");

    await waitFor(() => expect((extraJson as HTMLTextAreaElement).value).toBe('{\n  "account": "1234"\n}'));
    expect((extraJson as HTMLTextAreaElement).value).not.toContain("insecure_mode");
  });
});
