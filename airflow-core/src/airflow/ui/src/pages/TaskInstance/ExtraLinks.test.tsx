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
import { useParams } from "react-router-dom";
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

import * as queries from "openapi/queries";
import { Wrapper } from "src/utils/Wrapper";

import { ExtraLinks } from "./ExtraLinks";

vi.mock("openapi/queries");
vi.mock("react-router-dom", async () => {
  const actual = await vi.importActual("react-router-dom");

  return {
    ...actual,
    useParams: vi.fn(),
  };
});

describe("ExtraLinks Component", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(useParams).mockReturnValue({
      dagId: "test-dag",
      mapIndex: "-1",
      runId: "test-run",
      taskId: "test-task",
    });
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("renders internal links with target='_self'", () => {
    vi.mocked(queries.useTaskInstanceServiceGetExtraLinks).mockReturnValue({
      data: {
        extra_links: {
          "Internal Link": "/dags/test/runs/run1",
          "Same Origin": "http://localhost:3000/some-path",
        },
      },
    } as unknown as ReturnType<typeof queries.useTaskInstanceServiceGetExtraLinks>);

    // Mock window.location.origin
    vi.stubGlobal("location", { origin: "http://localhost:3000" });

    render(
      <Wrapper>
        <ExtraLinks refetchInterval={false} />
      </Wrapper>,
    );

    const internalLink = screen.getByText("Internal Link");

    expect(internalLink.closest("a")).toHaveAttribute("target", "_self");

    const sameOriginLink = screen.getByText("Same Origin");

    expect(sameOriginLink.closest("a")).toHaveAttribute("target", "_self");
  });

  it("renders external links with target='_blank'", () => {
    vi.mocked(queries.useTaskInstanceServiceGetExtraLinks).mockReturnValue({
      data: {
        extra_links: {
          "External Link": "https://www.google.com",
        },
      },
    } as unknown as ReturnType<typeof queries.useTaskInstanceServiceGetExtraLinks>);

    // Mock window.location.origin
    vi.stubGlobal("location", { origin: "http://localhost:3000" });

    render(
      <Wrapper>
        <ExtraLinks refetchInterval={false} />
      </Wrapper>,
    );

    const externalLink = screen.getByText("External Link");

    expect(externalLink.closest("a")).toHaveAttribute("target", "_blank");
  });

  it("filters out null urls", () => {
    vi.mocked(queries.useTaskInstanceServiceGetExtraLinks).mockReturnValue({
      data: {
        extra_links: {
          Invalid: null,
          Valid: "http://localhost:3000/valid",
        },
      },
    } as unknown as ReturnType<typeof queries.useTaskInstanceServiceGetExtraLinks>);

    render(
      <Wrapper>
        <ExtraLinks refetchInterval={false} />
      </Wrapper>,
    );

    expect(screen.getByText("Valid")).toBeInTheDocument();
    expect(screen.queryByText("Invalid")).not.toBeInTheDocument();
  });
});
