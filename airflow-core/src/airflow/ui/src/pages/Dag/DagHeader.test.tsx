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
import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import i18n from "i18next";
import { initReactI18next } from "react-i18next";
import { beforeAll, describe, it, expect } from "vitest";

import type { DAGDetailsResponse, DagVersionResponse } from "openapi/requests/types.gen";

import { MOCK_DAG } from "src/mocks/handlers/dag";
import { Wrapper } from "src/utils/Wrapper";

import { Header } from "./Header";

beforeAll(async () => {
  await i18n.use(initReactI18next).init({
    lng: "en",
    resources: {
      en: {
        common: { dagDetails: { bundleVersion: "Bundle Version", latestDagVersion: "Latest Dag Version" } },
        components: { versionSelect: { versionCode: "v{{versionCode}}" } },
      },
    },
  });
});

const storedVersion: DagVersionResponse = {
  bundle_name: "git-bundle",
  bundle_url: "https://example.com/source/tree/1234567890abcdef1234567890abcdef12345678",
  bundle_version: "1234567890abcdef1234567890abcdef12345678",
  created_at: "2026-01-01T00:00:00Z",
  dag_display_name: MOCK_DAG.dag_display_name,
  dag_id: MOCK_DAG.dag_id,
  id: "019b76da-a800-7000-8000-000000000000",
  version_number: 1,
};

const dagWithVersion = {
  ...MOCK_DAG,
  bundle_version: "newer-fetched-version",
  latest_dag_version: storedVersion,
} as unknown as DAGDetailsResponse;

describe("Dag header bundle version", () => {
  it.each([false, true])(
    "shows the stored bundle version and matching link when is_stale is %s",
    (isStale) => {
      render(<Header dag={{ ...dagWithVersion, has_import_errors: isStale, is_stale: isStale }} />, {
        wrapper: Wrapper,
      });

      expect(screen.queryByText("Bundle Version")).not.toBeInTheDocument();
      const versionStat = screen
        .getAllByTestId("stat")
        .find((stat) => within(stat).queryByText("Latest Dag Version"));

      expect(versionStat).toBeDefined();
      expect(versionStat).toHaveTextContent("v1");
      expect(versionStat).toHaveTextContent("1234567");
      const link = screen.getByRole("link", { name: "1234567" });

      expect(link).toHaveAttribute("href", storedVersion.bundle_url);
      expect(link).toHaveAttribute("target", "_blank");
      expect(link).toHaveAttribute("rel", "noopener noreferrer");
      expect(screen.queryByText("newer-fetched-version")).not.toBeInTheDocument();
      expect(screen.getByTestId("dag-version")).toHaveTextContent("v1");
    },
  );

  it.each([
    "release-2026-11-20-production",
    "a".repeat(39),
    "a".repeat(41),
    "g".repeat(40),
    "a".repeat(63),
    "a".repeat(65),
  ])("shows non-SHA version %s in full without inventing a link", (version) => {
    render(
      <Header
        dag={{
          ...dagWithVersion,
          latest_dag_version: { ...storedVersion, bundle_url: null, bundle_version: version },
        }}
      />,
      { wrapper: Wrapper },
    );

    expect(screen.queryByText("Bundle Version")).not.toBeInTheDocument();
    expect(screen.getByText(version)).toBeVisible();
    expect(screen.getByText(version)).not.toHaveAttribute("tabindex");
    expect(screen.queryByRole("link", { name: version })).not.toBeInTheDocument();
  });

  it("preserves a non-SHA source link and its full version label", () => {
    const version = "release-2026-11-20-production";

    render(
      <Header
        dag={{ ...dagWithVersion, latest_dag_version: { ...storedVersion, bundle_version: version } }}
      />,
      { wrapper: Wrapper },
    );

    expect(screen.getByRole("link", { name: version })).toHaveAttribute("href", storedVersion.bundle_url);
  });

  it.each(
    ["1234567890abcdef1234567890abcdef12345678", "ABCDEF12".repeat(8)].flatMap((version) => [
      { interaction: "hover", linked: true, version },
      { interaction: "focus", linked: true, version },
      { interaction: "hover", linked: false, version },
      { interaction: "focus", linked: false, version },
    ]),
  )("reveals $version on $interaction with source link $linked", async ({ interaction, linked, version }) => {
    render(
      <Header
        dag={{
          ...dagWithVersion,
          latest_dag_version: {
            ...storedVersion,
            bundle_url: linked ? storedVersion.bundle_url : null,
            bundle_version: version,
          },
        }}
      />,
      { wrapper: Wrapper },
    );

    const trigger = screen.getByText(version.slice(0, 7));

    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
    if (linked) {
      expect(screen.getByRole("link", { name: version.slice(0, 7) })).toHaveAttribute(
        "href",
        storedVersion.bundle_url,
      );
    } else {
      expect(screen.queryByRole("link", { name: version.slice(0, 7) })).not.toBeInTheDocument();
      expect(trigger).toHaveAttribute("tabindex", "0");
    }

    if (interaction === "hover") {
      fireEvent.pointerEnter(trigger, { pointerType: "mouse" });
    } else {
      fireEvent.keyDown(document, { key: "Tab" });
      act(() => trigger.focus());
      expect(trigger).toHaveFocus();
    }

    expect(await screen.findByRole("tooltip")).toHaveTextContent(version);
    expect(trigger).toHaveAccessibleDescription(version);
  });

  it.each([
    ["loading", undefined],
    ["no stored Dag version", { ...dagWithVersion, latest_dag_version: null }],
    [
      "unversioned bundle",
      { ...dagWithVersion, latest_dag_version: { ...storedVersion, bundle_version: null } },
    ],
  ] as const)("omits the bundle version for %s", (_state, dag) => {
    render(<Header dag={dag} />, { wrapper: Wrapper });

    expect(screen.queryByText("Bundle Version")).not.toBeInTheDocument();
    expect(screen.queryByText("newer-fetched-version")).not.toBeInTheDocument();
    expect(screen.queryByText("1234567")).not.toBeInTheDocument();
  });
});

describe("Dag Documentation Modal", () => {
  it("Display documentation button when doc_md is present", async () => {
    render(
      <Wrapper>
        <Header dag={MOCK_DAG as unknown as DAGDetailsResponse} />
      </Wrapper>,
    );

    await waitFor(() => expect(screen.getByTestId("markdown-button")).toBeInTheDocument());
    await waitFor(() => screen.getByTestId("markdown-button").click());
    await waitFor(() =>
      expect(screen.getByText(/taskflow api tutorial documentation/iu)).toBeInTheDocument(),
    );
  });

  it("Do not display documentation button only doc_md is not present", () => {
    render(
      <Wrapper>
        <Header dag={{ ...MOCK_DAG, doc_md: null } as unknown as DAGDetailsResponse} />
      </Wrapper>,
    );

    expect(screen.queryByTestId("markdown-button")).toBeNull();
  });
});
