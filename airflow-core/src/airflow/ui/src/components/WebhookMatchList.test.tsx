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
import { render, screen, within } from "@testing-library/react";
import { beforeEach, describe, expect, it } from "vitest";

import i18n from "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import commonLocale from "../../public/i18n/locales/en/common.json";
import { WebhookMatchList } from "./WebhookMatchList";

describe("WebhookMatchList", () => {
  // Without the bundle i18n.t() echoes the key, so the text assertions below would pass blindly.
  beforeEach(() => {
    i18n.addResourceBundle("en", "common", commonLocale, true, true);
  });

  it("shows each condition's webhook, events, match and conf key", () => {
    render(
      <WebhookMatchList
        matches={[
          {
            correlation_key_conf: "pr",
            events: ["pull_request", "push"],
            match: { action: ["closed"], repository: ["my-org/shop", "my-org/web"] },
            webhook: "github",
          },
          { correlation_key_conf: null, events: null, match: {}, webhook: "slack" },
        ]}
      />,
      { wrapper: Wrapper },
    );

    const [github, slack] = screen.getAllByTestId("webhook-match") as [HTMLElement, HTMLElement];

    expect(within(github).getByRole("link", { name: "github" })).toHaveAttribute(
      "href",
      "/webhook_deliveries/github",
    );
    expect(within(github).getByText("pull_request, push")).toBeInTheDocument();
    expect(
      within(within(github).getByTestId("webhook-match-attributes"))
        .getAllByRole("row")
        .map((row) => row.textContent),
    ).toEqual(["actionclosed", "repositoryOne of my-org/shop, my-org/web"]);
    expect(
      within(github).getByText(
        "Also the runs triggered with the delivery's correlation key as pr in their conf",
      ),
    ).toBeInTheDocument();

    expect(within(slack).getByRole("link", { name: "slack" })).toHaveAttribute(
      "href",
      "/webhook_deliveries/slack",
    );
    expect(within(slack).getByText("Any event")).toBeInTheDocument();
    expect(within(slack).getByText("Any attributes")).toBeInTheDocument();
    expect(within(slack).queryByTestId("webhook-match-attributes")).not.toBeInTheDocument();
    expect(within(slack).queryByText(/in their conf/u)).not.toBeInTheDocument();
  });
});
