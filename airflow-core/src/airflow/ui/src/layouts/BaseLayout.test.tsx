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
import { render, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { useDocumentTitle } from "src/utils";
import { Wrapper } from "src/utils/Wrapper";

import { BaseLayout } from "./BaseLayout";

const mockConfig: Record<string, unknown> = {
  instance_name: "Market Data Dev (SpectroCloud)",
};

vi.mock("openapi/queries", () => ({
  usePluginServiceGetPlugins: () => ({ data: { plugins: [] } }),
}));

vi.mock("src/queries/useConfig", () => ({
  useConfig: (key: string) => mockConfig[key],
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    i18n: {
      dir: () => "ltr",
      language: "en",
      off: vi.fn(),
      on: vi.fn(),
    },
    // eslint-disable-next-line id-length
    t: (key: string) => key,
  }),
}));

vi.mock("./Nav", () => ({
  Nav: () => <nav />,
}));

const DagPage = () => {
  useDocumentTitle("example_dag");

  return <div />;
};

describe("BaseLayout", () => {
  beforeEach(() => {
    document.title = "Airflow";
    mockConfig.instance_name = "Market Data Dev (SpectroCloud)";
  });

  afterEach(() => {
    document.title = "Airflow";
  });

  it("uses instance_name as the browser title for non-Dag pages", async () => {
    render(<BaseLayout />, { wrapper: Wrapper });

    await waitFor(() => expect(document.title).toBe("Market Data Dev (SpectroCloud)"));
  });

  it("lets Dag pages add their page title before instance_name", async () => {
    render(
      <BaseLayout>
        <DagPage />
      </BaseLayout>,
      { wrapper: Wrapper },
    );

    await waitFor(() => expect(document.title).toBe("example_dag - Market Data Dev (SpectroCloud)"));
  });

  it("restores the instance_name title when leaving a titled page", async () => {
    const { rerender } = render(
      <BaseLayout>
        <DagPage />
      </BaseLayout>,
      { wrapper: Wrapper },
    );

    await waitFor(() => expect(document.title).toBe("example_dag - Market Data Dev (SpectroCloud)"));

    rerender(<BaseLayout />);

    await waitFor(() => expect(document.title).toBe("Market Data Dev (SpectroCloud)"));
  });

  it.each([[""], [undefined]])("falls back to Airflow when instance_name is %s", async (instanceName) => {
    mockConfig.instance_name = instanceName;

    render(<BaseLayout />, { wrapper: Wrapper });

    await waitFor(() => expect(document.title).toBe("Airflow"));
  });
});
